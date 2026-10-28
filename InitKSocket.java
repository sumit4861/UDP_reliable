import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * InitKSocket: The initialization daemon process for KTP sockets.
 *
 * Per assignment PDF:
 * 1. Initializes shared memory chunk SM containing information about N KTP sockets.
 * 2. Starts Thread R (receiver thread) to handle incoming messages from UDP sockets.
 * 3. Starts Thread S (sender thread) to handle timeouts and retransmissions.
 * 4. Starts the Garbage Collector to clean up orphaned entries if processes terminate.
 * 5. Provides IPC service on port 10039 so separate user applications (User1, User2)
 *    can interact with the shared KTP sockets.
 */
public class InitKSocket {
    public static final byte CMD_SOCKET = 1;
    public static final byte CMD_BIND = 2;
    public static final byte CMD_SEND = 3;
    public static final byte CMD_RECV = 4;
    public static final byte CMD_CLOSE = 5;
    public static final byte CMD_STATS = 6;
    public static final byte CMD_SET_PARAMS = 7;
    public static final byte CMD_ALL_ACKED = 8;

    private final KtpSharedMemory shm;
    private final ThreadR threadR;
    private final ThreadS threadS;
    private final GarbageCollector gc;
    private ServerSocket serverSocket;
    private volatile boolean running = true;

    public InitKSocket() {
        this.shm = KtpSharedMemory.getInstance();
        this.threadS = new ThreadS(shm);
        this.threadR = new ThreadR(shm, threadS);
        this.gc = new GarbageCollector(shm);
    }

    public void start() {
        System.out.println("==================================================");
        System.out.println("  KTP Socket Daemon (InitKSocket) Starting...     ");
        System.out.println("==================================================");
        System.out.println("[InitKSocket] Initializing Shared Memory (N=" + KtpConstants.MAX_KTP_SOCKETS + " sockets)...");
        System.out.println("[InitKSocket] Protocol Settings: T = " + KtpConstants.T + "s, p = " + KtpConstants.P);

        threadR.start();
        System.out.println("[InitKSocket] Thread R (Receiver Thread) started.");

        threadS.start();
        System.out.println("[InitKSocket] Thread S (Sender/Retransmit Thread) started.");

        gc.start();
        System.out.println("[InitKSocket] Garbage Collector started.");

        try {
            serverSocket = new ServerSocket(KtpConstants.DEFAULT_IPC_PORT);
            System.out.println("[InitKSocket] IPC Service listening on port " + KtpConstants.DEFAULT_IPC_PORT);
            System.out.println("[InitKSocket] Ready for user applications (User1, User2).\n");

            // Background IPC listener
            Thread ipcThread = new Thread(this::listenForClients, "KTP-IPC-Listener");
            ipcThread.setDaemon(true);
            ipcThread.start();

        } catch (Exception e) {
            System.err.println("[InitKSocket] Failed to bind IPC port: " + e.getMessage());
            System.exit(1);
        }
    }

    private void listenForClients() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClient(client), "KTP-Client-Handler").start();
            } catch (Exception e) {
                if (!running) break;
            }
        }
    }

    private void handleClient(Socket client) {
        int allocatedSocketId = -1;
        long clientPid = 0;
        try (DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            while (running && !client.isClosed()) {
                byte cmd = in.readByte();
                switch (cmd) {
                    case CMD_SOCKET: {
                        clientPid = in.readLong();
                        int fd = shm.allocateSocket(clientPid);
                        if (fd >= 0) {
                            allocatedSocketId = fd;
                        }
                        out.writeInt(fd);
                        out.flush();
                        break;
                    }
                    case CMD_BIND: {
                        int fd = in.readInt();
                        String localHost = in.readUTF();
                        int localPort = in.readInt();
                        String remoteHost = in.readUTF();
                        int remotePort = in.readInt();
                        long pid = in.readLong();

                        InetSocketAddress local = new InetSocketAddress(InetAddress.getByName(localHost), localPort);
                        InetSocketAddress remote = new InetSocketAddress(InetAddress.getByName(remoteHost), remotePort);

                        int res = shm.bindSocket(fd, local, remote, pid);
                        out.writeInt(res);
                        out.flush();
                        break;
                    }
                    case CMD_SEND: {
                        int fd = in.readInt();
                        int len = in.readInt();
                        if (len <= 0 || len > KtpConstants.MAX_MESSAGE_SIZE) {
                            out.writeInt(-KtpConstants.EINVAL);
                            out.flush();
                            break;
                        }
                        byte[] data = new byte[len];
                        in.readFully(data);

                        KtpSocketEntry entry = shm.getSocket(fd);
                        if (entry == null || !entry.isAllotted) {
                            out.writeInt(-KtpConstants.ERRORNP);
                        } else if (!entry.isBound) {
                            out.writeInt(-KtpConstants.ENOTBOUND);
                        } else {
                            entry.lock.lock();
                            try {
                                boolean queued = entry.swnd.enqueue(data, len);
                                if (queued) {
                                    entry.lastActiveTime = System.currentTimeMillis();
                                    threadS.triggerSend();
                                    out.writeInt(len);
                                } else {
                                    out.writeInt(-KtpConstants.ENOSPACE);
                                }
                            } finally {
                                entry.lock.unlock();
                            }
                        }
                        out.flush();
                        break;
                    }
                    case CMD_RECV: {
                        int fd = in.readInt();
                        int maxLen = in.readInt();

                        KtpSocketEntry entry = shm.getSocket(fd);
                        if (entry == null || !entry.isAllotted) {
                            out.writeInt(-KtpConstants.ERRORNP);
                        } else if (!entry.isBound) {
                            out.writeInt(-KtpConstants.ENOTBOUND);
                        } else {
                            byte[] msg = entry.rwnd.peekMessage();
                            if (msg != null && (maxLen < msg.length || maxLen <= 0)) {
                                out.writeInt(-KtpConstants.ETOOSMALL);
                            } else if (msg != null) {
                                entry.rwnd.readMessage();
                                entry.lastActiveTime = System.currentTimeMillis();
                                out.writeInt(msg.length);
                                out.write(msg);
                            } else {
                                out.writeInt(-KtpConstants.ENOMESSAGE);
                            }
                        }
                        out.flush();
                        break;
                    }
                    case CMD_CLOSE: {
                        int fd = in.readInt();
                        long pid = in.readLong();
                        int res = shm.closeSocket(fd, pid);
                        if (fd == allocatedSocketId) {
                            allocatedSocketId = -1;
                        }
                        out.writeInt(res);
                        out.flush();
                        break;
                    }
                    case CMD_STATS: {
                        int fd = in.readInt();
                        KtpSocketEntry entry = shm.getSocket(fd);
                        if (entry != null) {
                            out.writeInt(entry.totalTransmissions.get());
                            out.writeInt(entry.totalDataMessages.get());
                        } else {
                            out.writeInt(0);
                            out.writeInt(0);
                        }
                        out.flush();
                        break;
                    }
                    case CMD_ALL_ACKED: {
                        int fd = in.readInt();
                        KtpSocketEntry entry = shm.getSocket(fd);
                        out.writeBoolean(entry != null && entry.isAllotted && entry.swnd.isAllAcknowledged());
                        out.flush();
                        break;
                    }
                    case CMD_SET_PARAMS: {
                        int newT = in.readInt();
                        double newP = in.readDouble();
                        KtpConstants.T = newT;
                        KtpConstants.P = newP;
                        out.writeInt(1);
                        out.flush();
                        break;
                    }
                    default:
                        System.err.println("[InitKSocket] Unknown IPC command: " + cmd);
                        break;
                }
            }
        } catch (Exception e) {
            // Client disconnected
        } finally {
            if (allocatedSocketId != -1) {
                System.out.println("[InitKSocket] Process disconnected, auto-closing socket " + allocatedSocketId);
                shm.closeSocket(allocatedSocketId, clientPid);
            }
        }
    }

    public static void main(String[] args) {
        InitKSocket daemon = new InitKSocket();
        daemon.start();

        System.out.println("Commands available: status | setp <p> | sett <T> | quit");
        Scanner sc = new Scanner(System.in);
        while (daemon.running) {
            try {
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("status")) {
                    daemon.printStatus();
                } else if (line.startsWith("setp ")) {
                    double p = Double.parseDouble(line.substring(5).trim());
                    KtpConstants.P = p;
                    System.out.println("Drop probability P updated to: " + p);
                } else if (line.startsWith("sett ")) {
                    int t = Integer.parseInt(line.substring(5).trim());
                    KtpConstants.T = t;
                    System.out.println("Timeout T updated to: " + t + "s");
                } else if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                    System.out.println("Stopping InitKSocket daemon...");
                    daemon.running = false;
                    break;
                }
            } catch (Exception e) {
                System.out.println("Command error: " + e.getMessage());
            }
        }
        System.exit(0);
    }

    private void printStatus() {
        System.out.println("\n--- KTP Socket Status Table ---");
        for (int i = 0; i < KtpConstants.MAX_KTP_SOCKETS; i++) {
            KtpSocketEntry e = shm.getSocket(i);
            if (e.isAllotted) {
                System.out.printf("Socket %d: Bound=%b, Local=%s, Remote=%s, Transmissions=%d, Messages=%d\n",
                        i, e.isBound, e.localAddr, e.remoteAddr, e.totalTransmissions.get(), e.totalDataMessages.get());
            }
        }
        System.out.println("--------------------------------\n");
    }
}
