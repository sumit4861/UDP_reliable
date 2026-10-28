import java.util.List;

/**
 * GarbageCollector runs periodically to clean up orphaned KTP sockets
 * if user applications terminate without calling k_close explicitly.
 */
public class GarbageCollector extends Thread {
    private final KtpSharedMemory shm;
    private volatile boolean running = true;
    private final long idleTimeoutMillis;

    public GarbageCollector(KtpSharedMemory shm) {
        this(shm, 60000L); // 60 seconds idle cleanup
    }

    public GarbageCollector(KtpSharedMemory shm, long idleTimeoutMillis) {
        super("KTP-GarbageCollector");
        this.shm = shm;
        this.idleTimeoutMillis = idleTimeoutMillis;
        setDaemon(true);
    }

    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(3000); // Check every 3 seconds
                long now = System.currentTimeMillis();

                List<KtpSocketEntry> active = shm.getAllActiveSockets();
                for (KtpSocketEntry entry : active) {
                    boolean ownerAlive = entry.processId > 0
                            && ProcessHandle.of(entry.processId).map(ProcessHandle::isAlive).orElse(false);
                    if (entry.isAllotted && !ownerAlive) {
                        System.out.println("[GarbageCollector] Cleaning up socket owned by terminated process " + entry.id +
                                " (process ID: " + entry.processId + ")");
                        shm.closeSocket(entry.id, entry.processId);
                    }
                }
            } catch (InterruptedException e) {
                if (!running) break;
            } catch (Exception e) {
                // System.err.println("[GarbageCollector] Error: " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        running = false;
        interrupt();
    }
}
