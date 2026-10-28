import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.List;

/**
 * Thread R: Receiver Thread
 * Waits for incoming messages on any of the UDP sockets using non-blocking NIO Selector.
 * Handles dropMessage simulation, receiver buffer queueing, cumulative ACK generation,
 * sliding sending window on ACK reception, and nospace recovery on select timeout.
 */
public class ThreadR extends Thread {
    private final KtpSharedMemory shm;
    private final ThreadS threadS;
    private volatile boolean running = true;
    private Selector selector;

    public ThreadR(KtpSharedMemory shm, ThreadS threadS) {
        super("KTP-Thread-R");
        this.shm = shm;
        this.threadS = threadS;
        setDaemon(true);
    }

    @Override
    public void run() {
        try {
            selector = Selector.open();
        } catch (Exception e) {
            System.err.println("[Thread R] Failed to open NIO Selector: " + e.getMessage());
            return;
        }

        ByteBuffer recvBuffer = ByteBuffer.allocate(2048);

        while (running) {
            try {
                // Update selector with any new or active channels
                registerChannels();

                // Wait for incoming packets with select timeout (100 ms)
                int readyChannels = selector.select(100);

                // On timeout or cycle, check nospace recovery for all active sockets
                checkNospaceRecoveryForAll();

                if (readyChannels == 0) {
                    continue;
                }

                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    if (!key.isValid() || !key.isReadable()) {
                        continue;
                    }

                    DatagramChannel channel = (DatagramChannel) key.channel();
                    KtpSocketEntry entry = (KtpSocketEntry) key.attachment();

                    recvBuffer.clear();
                    SocketAddress senderAddr = channel.receive(recvBuffer);
                    if (senderAddr == null) {
                        continue;
                    }

                    // Only accept datagrams from the configured peer for this KTP socket.
                    if (!(senderAddr instanceof InetSocketAddress) || entry.remoteAddr == null
                            || !entry.remoteAddr.equals(senderAddr)) {
                        continue;
                    }

                    recvBuffer.flip();
                    int bytesRead = recvBuffer.remaining();
                    if (bytesRead < KtpPacket.HEADER_SIZE) {
                        continue;
                    }

                    // Simulated unreliable channel: call dropMessage(p)
                    if (KtpConstants.dropMessage(KtpConstants.P)) {
                        // System.out.println("[Thread R] >> Message DROPPED by dropMessage(p=" + KtpConstants.P + ")");
                        continue;
                    }

                    byte[] raw = new byte[bytesRead];
                    recvBuffer.get(raw);
                    KtpPacket packet = KtpPacket.fromBytes(raw, 0, bytesRead);

                    if (packet == null || !packet.verifyChecksum()) {
                        // Checksum error or malformed packet
                        continue;
                    }

                    entry.lastActiveTime = System.currentTimeMillis();

                    // Process according to packet type
                    switch (packet.type) {
                        case KtpConstants.TYPE_DATA:
                        case KtpConstants.TYPE_RETRANSMIT: {
                            KtpPacket ackToSend = entry.rwnd.processDataPacket(packet);
                            if (ackToSend != null) {
                                sendUdpPacket(channel, entry.remoteAddr, ackToSend);
                            }
                            break;
                        }
                        case KtpConstants.TYPE_ACK: {
                            // ACK received in response to sent data
                            entry.swnd.processAck(packet.ackNum, packet.rwnd);
                            if (threadS != null) {
                                threadS.triggerSend();
                            }
                            break;
                        }
                        case KtpConstants.TYPE_WINDOW_PROBE: {
                            // Zero window probe from sender: reply with current ACK and rwnd
                            KtpPacket ackToSend = entry.rwnd.createCurrentAck();
                            sendUdpPacket(channel, entry.remoteAddr, ackToSend);
                            break;
                        }
                        case KtpConstants.TYPE_FIN: {
                            KtpPacket finAck = KtpPacket.createFinAckPacket(packet.seqNum);
                            sendUdpPacket(channel, entry.remoteAddr, finAck);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    // System.err.println("[Thread R] Warning in loop: " + e.getMessage());
                }
            }
        }

        try {
            if (selector != null) selector.close();
        } catch (Exception ignored) {}
    }

    private void registerChannels() {
        List<KtpSocketEntry> active = shm.getAllActiveSockets();
        for (KtpSocketEntry entry : active) {
            entry.lock.lock();
            try {
                if (entry.udpChannel != null && entry.udpChannel.isOpen()) {
                    SelectionKey key = entry.udpChannel.keyFor(selector);
                    if (key == null) {
                        entry.udpChannel.register(selector, SelectionKey.OP_READ, entry);
                    }
                }
            } catch (Exception ignored) {
            } finally {
                entry.lock.unlock();
            }
        }
    }

    private void checkNospaceRecoveryForAll() {
        List<KtpSocketEntry> active = shm.getAllActiveSockets();
        for (KtpSocketEntry entry : active) {
            KtpPacket recoveryAck = entry.rwnd.checkNospaceRecovery();
            if (recoveryAck != null && entry.remoteAddr != null) {
                entry.lock.lock();
                try {
                    if (entry.udpChannel != null && entry.udpChannel.isOpen()) {
                        sendUdpPacket(entry.udpChannel, entry.remoteAddr, recoveryAck);
                    }
                } finally {
                    entry.lock.unlock();
                }
            }
        }
    }

    private void sendUdpPacket(DatagramChannel channel, InetSocketAddress remote, KtpPacket pkt) {
        if (remote == null || channel == null || !channel.isOpen()) return;
        try {
            byte[] bytes = pkt.toBytes();
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            channel.send(bb, remote);
        } catch (Exception e) {
            // System.err.println("[Thread R] Error sending UDP packet: " + e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
    }
}
