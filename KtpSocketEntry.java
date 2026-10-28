import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * KtpSocketEntry represents an entry SM[i] in the Shared Memory chunk.
 * Contains socket state, UDP mappings, endpoints, send/receive windows, and stats.
 */
public class KtpSocketEntry {
    public final int id;
    public volatile boolean isAllotted = false;
    public volatile boolean isBound = false;
    public volatile long processId = 0;

    public DatagramSocket udpSocket = null;
    public DatagramChannel udpChannel = null;

    public InetSocketAddress localAddr = null;
    public InetSocketAddress remoteAddr = null;

    public final SendWindow swnd = new SendWindow();
    public final ReceiveWindow rwnd = new ReceiveWindow();

    public final AtomicInteger totalTransmissions = new AtomicInteger(0);
    public final AtomicInteger totalDataMessages = new AtomicInteger(0);
    public volatile long lastActiveTime = System.currentTimeMillis();

    public final ReentrantLock lock = new ReentrantLock();

    public KtpSocketEntry(int id) {
        this.id = id;
    }

    public void reset() {
        lock.lock();
        try {
            isAllotted = false;
            isBound = false;
            processId = 0;
            localAddr = null;
            remoteAddr = null;
            totalTransmissions.set(0);
            totalDataMessages.set(0);
            lastActiveTime = System.currentTimeMillis();

            if (udpChannel != null && udpChannel.isOpen()) {
                try {
                    udpChannel.close();
                } catch (Exception ignored) {}
            }
            if (udpSocket != null && !udpSocket.isClosed()) {
                try {
                    udpSocket.close();
                } catch (Exception ignored) {}
            }
            udpChannel = null;
            udpSocket = null;
        } finally {
            lock.unlock();
        }
    }
}
