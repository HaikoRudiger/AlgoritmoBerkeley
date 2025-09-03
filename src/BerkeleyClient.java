import java.io.*;
import java.net.*;

public class BerkeleyClient {

    private final String host;
    private final int port;
    private final String clientId;
    private final LogicalClock clock;

    public BerkeleyClient(String host, int port, String clientId, long initialOffsetMillis) {
        this.host = host;
        this.port = port;
        this.clientId = clientId;
        this.clock = new LogicalClock(initialOffsetMillis);
    }

    public void start() throws IOException {
        System.out.println("[CLIENT " + clientId + "] Conectando a " + host + ":" + port + " | " + clock);
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true)) {

            socket.setSoTimeout(0);

            out.println("HELLO " + clientId);

            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("TIME_REQUEST ")) {
                    long serverTime = Long.parseLong(line.substring("TIME_REQUEST ".length()).trim());
                    long myTime = clock.now();
                    long delta = myTime - serverTime;
                    out.println("OFFSET " + delta);
                    System.out.printf("[CLIENT %s] Solicitação recebida. Meu tempo=%s, delta=%+d ms%n",
                            clientId, clock.prettyNow(), delta);

                } else if (line.startsWith("ADJUST ")) {
                    long adjust = Long.parseLong(line.substring("ADJUST ".length()).trim());
                    clock.adjust(adjust);
                    System.out.printf("[CLIENT %s] Ajuste aplicado: %+d ms | nova hora=%s | offset=%+d ms%n",
                            clientId, adjust, clock.prettyNow(), clock.getOffset());

                }
            }
        }
    }

    // --- main ---
    public static void main(String[] args) throws Exception {
        // Uso: java BerkeleyClient [host] [porta] [clientId] [offsetInicialMs]
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
        String clientId = args.length > 2 ? args[2] : ("cli-" + (int)(Math.random()*1000));
        long initialOffset = args.length > 3 ? Long.parseLong(args[3]) : (long)(Math.random()*2000 - 1000); // ±1s

        new BerkeleyClient(host, port, clientId, initialOffset).start();
    }
}
