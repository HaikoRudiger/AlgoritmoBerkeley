import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
        var serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);

        prepareThreadForClientConnections(serverSocket);
        serverSocket.close();

        scheduleSynchronization();

        System.out.println("[MASTER] Aguardando clientes... (CTRL+C para sair)");
    }

    private void prepareThreadForClientConnections(ServerSocket serverSocket) {
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
    }

    private void scheduleSynchronization() {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                synchronizeOnce();
            } catch (Exception e) {
                System.out.println("[MASTER] Erro no ciclo de sync: " + e.getMessage());
            }
        }, 3, syncIntervalSec, TimeUnit.SECONDS);
    }

    private void synchronizeOnce() {
        int cycleNumber = cycle.incrementAndGet();
        if (clients.isEmpty()) {
            System.out.println("[MASTER][Ciclo " + cycleNumber + "] Sem clientes conectados; pulando.");
            return;
        }

        System.out.println("\n[MASTER][Ciclo " + cycleNumber + "] ===== Início =====");
        long timeServer = clock.now();
        System.out.println("[MASTER] Hora atual (lógica): " + clock.prettyNow() + " | offset=" + clock.getOffset() + " ms");
        System.out.println("[MASTER] Requisitando offsets a " + clients.size() + " cliente(s)...");

        var deltas = getClientsDelta(timeServer);
        if (deltas.isEmpty()) {
            System.out.println("[MASTER] Nenhum delta recebido; encerrando ciclo.");
            return;
        }

        var deltasAverage = calculateDeltasAverage(deltas);

        sendAdjustmentsToClients(deltas, deltasAverage, cycleNumber);

        clock.adjust(deltasAverage);
        System.out.println("[MASTER] Ajuste aplicado no master: " + deltasAverage + " ms | nova hora=" + clock.prettyNow());
        System.out.println("[MASTER][Ciclo " + cycleNumber + "] ===== Fim =====\n");
    }

    private Map<ClientHandler, Long> getClientsDelta(Long timeServer) {
        Map<ClientHandler, Long> deltas = new LinkedHashMap<>();
        Iterator<ClientHandler> clientsIterator = clients.iterator();
        while (clientsIterator.hasNext()) {
            ClientHandler clientHandler = clientsIterator.next();
            try {
                long delta = clientHandler.requestOffset(timeServer);
                deltas.put(clientHandler, delta);
                System.out.printf("[MASTER]  - %s delta=%d ms%n", clientHandler, delta);
            } catch (IOException e) {
                System.out.println("[MASTER]  - Removendo cliente desconectado: " + clientHandler);
                clientsIterator.remove();
                clientHandler.closeQuietly();
            }
        }

        return deltas;
    }

    private long calculateDeltasAverage(Map<ClientHandler, Long> deltas) {
        long sum = 0;
        int count = 1;
        for (long d : deltas.values()) {
            sum += d;
            count++;
        }
        long avg = Math.round((double) sum / count);
        System.out.printf("[MASTER] Média dos deltas (incluindo master=0): %d ms%n", avg);

        return avg;
    }

    private void sendAdjustmentsToClients(Map<ClientHandler, Long> deltas, long deltasAverage, int cycleNumber) {
        for (Map.Entry<ClientHandler, Long> e : deltas.entrySet()) {
            long adjust = deltasAverage - e.getValue();
            try {
                e.getKey().sendAdjust(adjust);
                System.out.printf("[MASTER]  -> Ajuste enviado a %s: %+d ms%n", e.getKey(), adjust);
            } catch (IOException ex) {
                System.out.println("[MASTER]  - Falha ao enviar ajuste; removendo " + e.getKey());
                clients.remove(e.getKey());
                e.getKey().closeQuietly();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Uso: java BerkeleyServer [porta] [intervaloSeg] [timeoutMs] [offsetInicialMs]
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        int interval = args.length > 1 ? Integer.parseInt(args[1]) : 10;     // a cada 10s
        int timeout = args.length > 2 ? Integer.parseInt(args[2]) : 4000;    // 4s de timeout de IO
        long initialOffset = args.length > 3 ? Long.parseLong(args[3]) : 0;  // offset lógico do master

        BerkeleyServer server = new BerkeleyServer(port, interval, timeout, initialOffset);
        server.start();

        Thread.currentThread().join();
    }
}
