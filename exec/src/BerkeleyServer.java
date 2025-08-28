import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Servidor (master) do algoritmo de Berkeley usando TCP.
 * Protocolo texto (uma linha por mensagem):
 *  - Cliente ao conectar:           "HELLO <clientId>"
 *  - Servidor requisita offset:     "TIME_REQUEST <serverTimeMillis>"
 *  - Cliente responde:              "OFFSET <deltaMillis>"
 *  - Servidor envia ajuste:         "ADJUST <adjustMillis>"
 *  - Servidor opcionalmente fecha:  "BYE"
 */
public class BerkeleyServer {

    private final int port;
    private final int syncIntervalSec;
    private final int ioTimeoutMillis;
    private final LogicalClock clock;
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();
    private final AtomicInteger cycle = new AtomicInteger(0);

    public BerkeleyServer(int port, int syncIntervalSec, int ioTimeoutMillis, long initialOffsetMillis) {
        this.port = port;
        this.syncIntervalSec = syncIntervalSec;
        this.ioTimeoutMillis = ioTimeoutMillis;
        this.clock = new LogicalClock(initialOffsetMillis);
    }

    public void start() throws IOException {
        System.out.println("[MASTER] Iniciando na porta " + port + " | " + clock);
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);

        // Thread de aceitação de clientes
        Thread acceptThread = new Thread(() -> {
            try {
                while (true) {
                    Socket s = serverSocket.accept();
                    s.setSoTimeout(ioTimeoutMillis);
                    ClientHandler handler = new ClientHandler(s);
                    clients.add(handler);
                    new Thread(handler, "client-" + handler.getId()).start();
                }
            } catch (IOException e) {
                System.out.println("[MASTER] Accept encerrado: " + e.getMessage());
            }
        }, "accept-thread");
        acceptThread.setDaemon(true);
        acceptThread.start();

        // Agendador de ciclos de sincronização
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                synchronizeOnce();
            } catch (Exception e) {
                System.out.println("[MASTER] Erro no ciclo de sync: " + e.getMessage());
                e.printStackTrace();
            }
        }, 3, syncIntervalSec, TimeUnit.SECONDS);

        // Mantém o servidor vivo
        System.out.println("[MASTER] Aguardando clientes... (CTRL+C para sair)");
    }

    /** Um ciclo completo do algoritmo de Berkeley. */
    private void synchronizeOnce() {
        int nCycle = cycle.incrementAndGet();
        if (clients.isEmpty()) {
            System.out.println("[MASTER][Ciclo " + nCycle + "] Sem clientes conectados; pulando.");
            return;
        }

        System.out.println("\n[MASTER][Ciclo " + nCycle + "] ===== Início =====");
        long tServer = clock.now();
        System.out.println("[MASTER] Hora atual (lógica): " + clock.prettyNow() + " | offset=" + clock.getOffset() + " ms");
        System.out.println("[MASTER] Requisitando offsets a " + clients.size() + " cliente(s)...");

        // Passo 1 e 2: solicitar e coletar diferenças (delta) dos clientes
        Map<ClientHandler, Long> deltas = new LinkedHashMap<>();
        Iterator<ClientHandler> it = clients.iterator();
        while (it.hasNext()) {
            ClientHandler ch = it.next();
            try {
                long delta = ch.requestOffset(tServer);
                deltas.put(ch, delta);
                System.out.printf("[MASTER]  - %s delta=%d ms%n", ch, delta);
            } catch (IOException e) {
                System.out.println("[MASTER]  - Removendo cliente desconectado: " + ch);
                it.remove();
                ch.closeQuietly();
            }
        }

        if (deltas.isEmpty()) {
            System.out.println("[MASTER] Nenhum delta recebido; encerrando ciclo.");
            return;
        }

        // Inclui o delta do servidor (0) e calcula média
        long sum = 0;
        int count = 1; // servidor
        for (long d : deltas.values()) {
            sum += d;
            count++;
        }
        long avg = Math.round((double) sum / count);
        System.out.printf("[MASTER] Média dos deltas (incluindo master=0): %d ms%n", avg);

        // Passo 4: enviar ajustes = média - deltaCliente
        for (Map.Entry<ClientHandler, Long> e : deltas.entrySet()) {
            long adjust = avg - e.getValue();
            try {
                e.getKey().sendAdjust(adjust);
                System.out.printf("[MASTER]  -> Ajuste enviado a %s: %+d ms%n", e.getKey(), adjust);
            } catch (IOException ex) {
                System.out.println("[MASTER]  - Falha ao enviar ajuste; removendo " + e.getKey());
                clients.remove(e.getKey());
                e.getKey().closeQuietly();
            }
        }

        // Passo 5: master também se ajusta para a média
        clock.adjust(avg);
        System.out.println("[MASTER] Ajuste aplicado no master: " + avg + " ms | nova hora=" + clock.prettyNow());
        System.out.println("[MASTER][Ciclo " + nCycle + "] ===== Fim =====\n");
    }

    /** Representa um cliente conectado. */
    private class ClientHandler implements Runnable {
        private final Socket socket;
        private final BufferedReader in;
        private final PrintWriter out;
        private String clientId = "unknown";

        ClientHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true);
        }

        String getId() {
            return clientId;
        }

        @Override
        public void run() {
            try {
                // Espera HELLO <id>
                String hello = in.readLine();
                if (hello == null || !hello.startsWith("HELLO ")) {
                    throw new IOException("Handshake inválido: " + hello);
                }
                clientId = hello.substring("HELLO ".length()).trim();
                System.out.println("[MASTER] Cliente conectado: " + this);
            } catch (IOException e) {
                System.out.println("[MASTER] Erro no handshake com " + this + ": " + e.getMessage());
                closeQuietly();
            }
        }

        long requestOffset(long tServer) throws IOException {
            out.println("TIME_REQUEST " + tServer);
            String line = in.readLine();
            if (line == null || !line.startsWith("OFFSET ")) {
                throw new IOException("Resposta inválida: " + line);
            }
            return Long.parseLong(line.substring("OFFSET ".length()).trim());
        }

        void sendAdjust(long adjust) throws IOException {
            out.println("ADJUST " + adjust);
        }

        void closeQuietly() {
            try { socket.close(); } catch (Exception ignored) {}
        }

        @Override
        public String toString() {
            InetAddress a = socket.getInetAddress();
            return "Client[" + clientId + "@" + (a != null ? a.getHostAddress() : "?") + ":" + socket.getPort() + "]";
        }
    }

    // --- main ---
    public static void main(String[] args) throws Exception {
        // Uso: java BerkeleyServer [porta] [intervaloSeg] [timeoutMs] [offsetInicialMs]
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        int interval = args.length > 1 ? Integer.parseInt(args[1]) : 10;     // a cada 10s
        int timeout = args.length > 2 ? Integer.parseInt(args[2]) : 4000;    // 4s de timeout de IO
        long initialOffset = args.length > 3 ? Long.parseLong(args[3]) : 0;  // offset lógico do master

        BerkeleyServer server = new BerkeleyServer(port, interval, timeout, initialOffset);
        server.start();

        // Mantém processo ativo
        Thread.currentThread().join();
    }
}
