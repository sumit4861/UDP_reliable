import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;

/**
 * Thread S: Sender / Retransmission Thread
 * Wakes up periodically (T/2), checks retransmission timeout (T) for all active
 * sockets, retransmits all packets in swnd if timed out, and pushes pending
 * messages from the sender buffer into the UDP socket within the window limits.
 */
public class ThreadS extends Thread {
    private final KtpSharedMemory shm;
    private volatile boolean running = true;

    private final Object signal = new Object();

    public ThreadS(KtpSharedMemory shm) {
        super("KTP-Thread-S");
        this.shm = shm;
        setDaemon(true);
    }

    public void triggerSend() {
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                synchronized (signal) {
                    signal.wait(50);
                }

                List<KtpSocketEntry> active = shm.getAllActiveSockets();
                for (KtpSocketEntry entry : active) {
                    processSocket(entry);
                }
            } catch (InterruptedException e) {
                if (!running) break;
            } catch (Exception e) {
                // System.err.println("[Thread S] Warning: " + e.getMessage());
            }
        }
    }

    private void processSocket(KtpSocketEntry entry) {
        entry.lock.lock();
        try {
            if (!entry.isAllotted || !entry.isBound || entry.remoteAddr == null ||
                entry.udpChannel == null || !entry.udpChannel.isOpen()) {
                return;
            }

            // 1. Check for Timeout and Retransmission
            long timeoutMillis = KtpConstants.T * 1000L;
            List<KtpPacket> toRetransmit = entry.swnd.checkTimeout(timeoutMillis);
            for (KtpPacket pkt : toRetransmit) {
                sendPacket(entry.udpChannel, entry.remoteAddr, pkt);
                entry.totalTransmissions.incrementAndGet();
            }

            // 2. Check for Zero-Window persistence probe
            KtpPacket probe = entry.swnd.checkZeroWindowProbe(timeoutMillis);
            if (probe != null) {
                sendPacket(entry.udpChannel, entry.remoteAddr, probe);
            }

            // 3. Send pending messages within swnd
            List<KtpPacket> toSend = entry.swnd.getPacketsToSend();
            for (KtpPacket pkt : toSend) {
                sendPacket(entry.udpChannel, entry.remoteAddr, pkt);
                entry.totalTransmissions.incrementAndGet();
                entry.totalDataMessages.incrementAndGet();
            }
        } finally {
            entry.lock.unlock();
        }
    }

    private void sendPacket(DatagramChannel channel, InetSocketAddress remote, KtpPacket pkt) {
        if (channel == null || !channel.isOpen() || remote == null) return;
        try {
            byte[] bytes = pkt.toBytes();
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            channel.send(bb, remote);
        } catch (Exception e) {
            // System.err.println("[Thread S] Error sending UDP packet: " + e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        interrupt();
    }
}
