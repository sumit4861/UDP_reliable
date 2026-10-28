import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * KtpSocket: The KTP Socket API Library.
 *
 * Implements the 5 core functions specified in the assignment PDF:
 * - k_socket()
 * - k_bind()
 * - k_sendto() / k_send()
 * - k_recvfrom() / k_recv()
 * - k_close()
 * Plus dropMessage(float p) and diagnostic stats.
 *
 * Connects to the InitKSocket daemon via local IPC (port 10039).
 * If the daemon is not running, falls back automatically to embedded mode
 * to guarantee execution in all environments.
 */
public class KtpSocket {
    // Thread-local error number matching C's errno
    public static final ThreadLocal<Integer> errno = ThreadLocal.withInitial(() -> 0);

    // IPC client connection state
    private static Socket ipcSocket = null;
    private static DataInputStream ipcIn = null;
    private static DataOutputStream ipcOut = null;
    private static boolean useEmbedded = false;

    // Embedded fallback engine
    private static KtpSharedMemory embeddedShm = null;
    private static ThreadR embeddedThreadR = null;
    private static ThreadS embeddedThreadS = null;
    private static GarbageCollector embeddedGc = null;

    private static synchronized void ensureConnection() {
        if (ipcSocket != null && !ipcSocket.isClosed() && ipcSocket.isConnected()) {
            return;
        }
        if (useEmbedded) {
            return;
        }

        try {
            ipcSocket = new Socket("127.0.0.1", KtpConstants.DEFAULT_IPC_PORT);
            ipcIn = new DataInputStream(ipcSocket.getInputStream());
            ipcOut = new DataOutputStream(ipcSocket.getOutputStream());
        } catch (Exception e) {
            // Daemon not running, start embedded engine
            startEmbeddedEngine();
        }
    }

    private static synchronized void startEmbeddedEngine() {
        if (!useEmbedded) {
            useEmbedded = true;
            embeddedShm = KtpSharedMemory.getInstance();
            embeddedThreadS = new ThreadS(embeddedShm);
            embeddedThreadR = new ThreadR(embeddedShm, embeddedThreadS);
            embeddedGc = new GarbageCollector(embeddedShm);

            embeddedThreadR.start();
            embeddedThreadS.start();
            embeddedGc.start();
            System.out.println("[KtpSocket] Daemon not detected on port " + KtpConstants.DEFAULT_IPC_PORT +
                    ", auto-started embedded KTP engine.");
        }
    }

    /**
     * k_socket: Opens a KTP socket.
     * Checks whether free space is available in SM and creates corresponding UDP socket.
     * Returns socket descriptor fd (>= 0), or -1 with errno set to ENOSPACE.
     */
    public static synchronized int k_socket() {
        ensureConnection();
        long pid = ProcessHandle.current().pid();

        if (useEmbedded) {
            int fd = embeddedShm.allocateSocket(pid);
            if (fd < 0) {
                errno.set(KtpConstants.ENOSPACE);
                return -1;
            }
            return fd;
        } else {
            try {
                ipcOut.writeByte(InitKSocket.CMD_SOCKET);
                ipcOut.writeLong(pid);
                ipcOut.flush();
                int res = ipcIn.readInt();
                if (res < 0) {
                    errno.set(KtpConstants.ENOSPACE);
                    return -1;
                }
                return res;
            } catch (Exception e) {
                // If IPC failed, switch to embedded and retry
                startEmbeddedEngine();
                return k_socket();
            }
        }
    }

    /**
     * k_bind: Binds the KTP socket with source IP/port and destination IP/port.
     */
    public static synchronized int k_bind(int fd, InetAddress localIp, int localPort,
                                          InetAddress remoteIp, int remotePort) {
        ensureConnection();
        long pid = ProcessHandle.current().pid();

        if (useEmbedded) {
            InetSocketAddress local = new InetSocketAddress(localIp, localPort);
            InetSocketAddress remote = new InetSocketAddress(remoteIp, remotePort);
            int res = embeddedShm.bindSocket(fd, local, remote, pid);
            if (res < 0) {
                errno.set(-res);
                return -1;
            }
            return 1;
        } else {
            try {
                ipcOut.writeByte(InitKSocket.CMD_BIND);
                ipcOut.writeInt(fd);
                ipcOut.writeUTF(localIp.getHostAddress());
                ipcOut.writeInt(localPort);
                ipcOut.writeUTF(remoteIp.getHostAddress());
                ipcOut.writeInt(remotePort);
                ipcOut.writeLong(pid);
                ipcOut.flush();

                int res = ipcIn.readInt();
                if (res < 0) {
                    errno.set(-res);
                    return -1;
                }
                return 1;
            } catch (Exception e) {
                errno.set(KtpConstants.EBIND);
                return -1;
            }
        }
    }

    /**
     * k_sendto / k_send: Writes message to sender side message buffer.
     * Returns number of bytes written, or -1 with errno set to ENOSPACE or ENOTBOUND.
     */
    public static synchronized int k_sendto(int fd, byte[] buf, int len) {
        if (buf == null || len <= 0 || len > buf.length || len > KtpConstants.MAX_MESSAGE_SIZE) {
            errno.set(KtpConstants.EINVAL);
            return -1;
        }
        ensureConnection();

        if (useEmbedded) {
            KtpSocketEntry entry = embeddedShm.getSocket(fd);
            if (entry == null || !entry.isAllotted) {
                errno.set(KtpConstants.ERRORNP);
                return -1;
            }
            if (!entry.isBound) {
                errno.set(KtpConstants.ENOTBOUND);
                return -1;
            }
            entry.lock.lock();
            try {
                boolean queued = entry.swnd.enqueue(buf, len);
                if (queued) {
                    entry.lastActiveTime = System.currentTimeMillis();
                    if (embeddedThreadS != null) {
                        embeddedThreadS.triggerSend();
                    }
                    return len;
                } else {
                    errno.set(KtpConstants.ENOSPACE);
                    return -1;
                }
            } finally {
                entry.lock.unlock();
            }
        } else {
            try {
                ipcOut.writeByte(InitKSocket.CMD_SEND);
                ipcOut.writeInt(fd);
                ipcOut.writeInt(len);
                ipcOut.write(buf, 0, len);
                ipcOut.flush();

                int res = ipcIn.readInt();
                if (res < 0) {
                    errno.set(-res);
                    return -1;
                }
                return res;
            } catch (Exception e) {
                errno.set(KtpConstants.ENOSPACE);
                return -1;
            }
        }
    }

    public static int k_send(int fd, byte[] buf, int len) {
        return k_sendto(fd, buf, len);
    }

    /**
     * k_recvfrom / k_recv: Looks up receiver-side message buffer.
     * Returns bytes read into buf, or -1 with errno set to ENOMESSAGE if buffer is empty.
     */
    public static synchronized int k_recvfrom(int fd, byte[] buf, int maxLen) {
        if (buf == null || maxLen <= 0) return -1;
        ensureConnection();

        if (useEmbedded) {
            KtpSocketEntry entry = embeddedShm.getSocket(fd);
            if (entry == null || !entry.isAllotted) {
                errno.set(KtpConstants.ERRORNP);
                return -1;
            }
            if (!entry.isBound) {
                errno.set(KtpConstants.ENOTBOUND);
                return -1;
            }
            byte[] msg = entry.rwnd.peekMessage();
            if (msg != null) {
                if (msg.length > maxLen || msg.length > buf.length) {
                    errno.set(KtpConstants.ETOOSMALL);
                    return -1;
                }
                entry.rwnd.readMessage();
                entry.lastActiveTime = System.currentTimeMillis();
                System.arraycopy(msg, 0, buf, 0, msg.length);
                return msg.length;
            } else {
                errno.set(KtpConstants.ENOMESSAGE);
                return -1;
            }
        } else {
            try {
                ipcOut.writeByte(InitKSocket.CMD_RECV);
                ipcOut.writeInt(fd);
                int capacity = Math.min(maxLen, buf.length);
                ipcOut.writeInt(capacity);
                ipcOut.flush();

                int res = ipcIn.readInt();
                if (res < 0) {
                    errno.set(-res);
                    return -1;
                }
                ipcIn.readFully(buf, 0, res);
                return res;
            } catch (Exception e) {
                errno.set(KtpConstants.ENOMESSAGE);
                return -1;
            }
        }
    }

    public static int k_recv(int fd, byte[] buf, int len) {
        return k_recvfrom(fd, buf, len);
    }

    /**
     * k_close: Closes socket and cleans up corresponding socket entry in SM.
     */
    public static synchronized int k_close(int fd) {
        ensureConnection();
        long pid = ProcessHandle.current().pid();

        if (useEmbedded) {
            int res = embeddedShm.closeSocket(fd, pid);
            return res;
        } else {
            try {
                ipcOut.writeByte(InitKSocket.CMD_CLOSE);
                ipcOut.writeInt(fd);
                ipcOut.writeLong(pid);
                ipcOut.flush();
                return ipcIn.readInt();
            } catch (Exception e) {
                return -1;
            }
        }
    }

    /**
     * Retrieves statistics (total transmissions, total data messages) for socket fd.
     */
    public static synchronized int[] avg_calc(int fd) {
        ensureConnection();
        if (useEmbedded) {
            KtpSocketEntry entry = embeddedShm.getSocket(fd);
            if (entry != null) {
                return new int[]{entry.totalTransmissions.get(), entry.totalDataMessages.get()};
            }
            return new int[]{0, 0};
        } else {
            try {
                ipcOut.writeByte(InitKSocket.CMD_STATS);
                ipcOut.writeInt(fd);
                ipcOut.flush();
                int trans = ipcIn.readInt();
                int msgs = ipcIn.readInt();
                return new int[]{trans, msgs};
            } catch (Exception e) {
                return new int[]{0, 0};
            }
        }
    }

    public static synchronized boolean isAllAcknowledged(int fd) {
        ensureConnection();
        if (useEmbedded) {
            KtpSocketEntry entry = embeddedShm.getSocket(fd);
            return entry != null && entry.swnd.isAllAcknowledged();
        }
        try {
            ipcOut.writeByte(InitKSocket.CMD_ALL_ACKED);
            ipcOut.writeInt(fd);
            ipcOut.flush();
            return ipcIn.readBoolean();
        } catch (Exception e) {
            errno.set(KtpConstants.ERRORNP);
            return false;
        }
    }

    public static synchronized void setProtocolParams(int t, double p) {
        ensureConnection();
        KtpConstants.T = t;
        KtpConstants.P = p;
        if (!useEmbedded) {
            try {
                ipcOut.writeByte(InitKSocket.CMD_SET_PARAMS);
                ipcOut.writeInt(t);
                ipcOut.writeDouble(p);
                ipcOut.flush();
                ipcIn.readInt();
            } catch (Exception ignored) {}
        }
    }

    /**
     * Simulates packet loss with probability p (0.0 to 1.0).
     */
    public static boolean dropMessage(double p) {
        return KtpConstants.dropMessage(p);
    }

    public static int getErrno() {
        return errno.get();
    }
}
