import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;
    private String clientId = "unknown";

    ClientHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
    }

    String getId() {
        return clientId;
    }

    @Override
    public void run() {
        try {
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
