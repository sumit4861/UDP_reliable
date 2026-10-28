import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * KtpSharedMemory manages the SM chunk containing information about N KTP sockets.
 * Provides mutual exclusion and lifecycle management for all active KTP sockets.
 */
public class KtpSharedMemory {
    private static KtpSharedMemory instance;
    private final KtpSocketEntry[] smTable;

    public KtpSharedMemory() {
        smTable = new KtpSocketEntry[KtpConstants.MAX_KTP_SOCKETS];
        for (int i = 0; i < smTable.length; i++) {
            smTable[i] = new KtpSocketEntry(i);
        }
    }

    public static synchronized KtpSharedMemory getInstance() {
        if (instance == null) {
            instance = new KtpSharedMemory();
        }
        return instance;
    }

    /**
     * k_socket: Checks whether free space is available in SM, creates UDP socket,
     * initializes SM with corresponding entries.
     * Returns socket ID or -1 (ENOSPACE).
     */
    public synchronized int allocateSocket(long processId) {
        for (int i = 0; i < smTable.length; i++) {
            KtpSocketEntry entry = smTable[i];
            entry.lock.lock();
            try {
                if (!entry.isAllotted) {
                    try {
                        entry.udpChannel = DatagramChannel.open();
                        entry.udpChannel.configureBlocking(false);
                        entry.udpSocket = entry.udpChannel.socket();
                        entry.udpSocket.setReuseAddress(true);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return -1;
                    }

                    entry.isAllotted = true;
                    entry.isBound = false;
                    entry.processId = processId;
                    entry.lastActiveTime = System.currentTimeMillis();
                    return i;
                }
            } finally {
                entry.lock.unlock();
            }
        }
        return -1;
    }

    /**
     * k_bind: Binds UDP socket with source IP and source port, and updates
     * corresponding SM with destination IP and destination port.
     */
    public int bindSocket(int id, InetSocketAddress localAddr, InetSocketAddress remoteAddr, long processId) {
        if (id < 0 || id >= smTable.length) {
            return -1;
        }
        KtpSocketEntry entry = smTable[id];
        entry.lock.lock();
        try {
            if (!entry.isAllotted) {
                return -1;
            }
            if (entry.processId != 0 && processId != 0 && entry.processId != processId) {
                return -KtpConstants.ERRORNP;
            }

            try {
                if (entry.udpChannel == null || !entry.udpChannel.isOpen()) {
                    entry.udpChannel = DatagramChannel.open();
                    entry.udpChannel.configureBlocking(false);
                    entry.udpSocket = entry.udpChannel.socket();
                    entry.udpSocket.setReuseAddress(true);
                }

                if (!entry.udpSocket.isBound()) {
                    entry.udpSocket.bind(localAddr);
                }
            } catch (Exception e) {
                System.err.println("[k_bind] Failed to bind to " + localAddr + ": " + e.getMessage());
                return -KtpConstants.EBIND;
            }

            entry.localAddr = localAddr;
            entry.remoteAddr = remoteAddr;
            entry.isBound = true;
            entry.lastActiveTime = System.currentTimeMillis();
            return 1;
        } finally {
            entry.lock.unlock();
        }
    }

    /**
     * k_close: Closes socket and cleans up corresponding socket entry in SM,
     * marks the entry as free.
     */
    public int closeSocket(int id, long processId) {
        if (id < 0 || id >= smTable.length) return -1;
        KtpSocketEntry entry = smTable[id];

        // Verify ownership without holding the lock while waiting for ACKs.
        entry.lock.lock();
        try {
            if (!entry.isAllotted || (entry.processId != 0 && entry.processId != processId)) return -1;
        } finally {
            entry.lock.unlock();
        }

        long waitStart = System.currentTimeMillis();
        while (!entry.swnd.isAllAcknowledged() && System.currentTimeMillis() - waitStart < 3000) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        entry.lock.lock();
        try {
            if (!entry.isAllotted || (entry.processId != 0 && entry.processId != processId)) return -1;
            entry.reset();
            return 1;
        } finally {
            entry.lock.unlock();
        }
    }

    public KtpSocketEntry getSocket(int id) {
        if (id < 0 || id >= smTable.length) return null;
        return smTable[id];
    }

    public List<KtpSocketEntry> getAllActiveSockets() {
        List<KtpSocketEntry> active = new ArrayList<>();
        for (KtpSocketEntry entry : smTable) {
            if (entry.isAllotted && entry.isBound) {
                active.add(entry);
            }
        }
        return active;
    }
}
