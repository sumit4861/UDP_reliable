import java.util.Arrays;

/** Dependency-free regression tests for KTP packet and window invariants. */
public final class ProtocolUnitTests {
    private static int assertions = 0;

    private static void check(boolean condition, String message) {
        assertions++;
        if (!condition) throw new AssertionError(message);
    }

    private static KtpPacket data(int seq, int value) {
        return KtpPacket.createDataPacket(seq, new byte[]{(byte) value}, 1);
    }

    public static void main(String[] args) {
        packetRoundTripAndMalformedInput();
        senderWindowAndStaleAck();
        sequenceNumberWraparound();
        receiverOrderingAndCapacity();
        System.out.println("PASS: " + assertions + " protocol assertions");
    }

    private static void packetRoundTripAndMalformedInput() {
        byte[] payload = new byte[KtpConstants.MAX_MESSAGE_SIZE];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 31);
        KtpPacket original = KtpPacket.createDataPacket(255, payload, payload.length);
        byte[] wire = original.toBytes();
        KtpPacket parsed = KtpPacket.fromBytes(wire, 0, wire.length);
        check(parsed != null, "valid packet must parse");
        check(parsed.verifyChecksum(), "round-trip checksum must verify");
        check(parsed.seqNum == 255, "8-bit sequence must survive serialization");
        check(Arrays.equals(payload, parsed.payload), "512-byte payload must survive serialization");

        byte[] badLength = wire.clone();
        badLength[4] = 3;
        badLength[5] = 0;
        check(KtpPacket.fromBytes(badLength, 0, badLength.length) == null,
                "oversized payload length must be rejected");
        check(KtpPacket.fromBytes(wire, 0, wire.length - 1) == null,
                "truncated datagram must be rejected");

        byte[] corrupted = wire.clone();
        corrupted[corrupted.length - 1] ^= 1;
        KtpPacket corruptPacket = KtpPacket.fromBytes(corrupted, 0, corrupted.length);
        check(corruptPacket != null && !corruptPacket.verifyChecksum(),
                "payload corruption must fail checksum verification");
        check(!new SendWindow().enqueue(new byte[513], 513), "oversized application message must be rejected");
    }

    private static void senderWindowAndStaleAck() {
        SendWindow sender = new SendWindow();
        for (int i = 0; i < KtpConstants.WINDOW_SIZE; i++) {
            check(sender.enqueue(new byte[]{(byte) i}, 1), "buffer should accept up to ten messages");
        }
        check(!sender.enqueue(new byte[]{99}, 1), "11th outstanding message must be rejected");
        check(sender.getPacketsToSend().size() == KtpConstants.WINDOW_SIZE, "initial window sends ten messages");
        check(sender.getAdvertisedRwnd() == KtpConstants.WINDOW_SIZE, "initial rwnd is ten");
        sender.processAck(200, 0);
        check(sender.getAdvertisedRwnd() == KtpConstants.WINDOW_SIZE,
                "stale ACK must not overwrite the advertised receive window");
        check(sender.processAck(5, 4) == 5, "cumulative ACK must remove five messages");
        check(sender.getInFlightCount() == 5, "five messages should remain in flight");
        check(sender.getAdvertisedRwnd() == 4, "valid ACK should update advertised rwnd");
    }

    private static void sequenceNumberWraparound() {
        SendWindow sender = new SendWindow();
        for (int i = 0; i < 256; i++) {
            check(sender.enqueue(new byte[]{(byte) i}, 1), "single message should enqueue");
            java.util.List<KtpPacket> packets = sender.getPacketsToSend();
            check(packets.size() == 1, "one message should be sent per cycle");
            int expectedSeq = (i + 1) % KtpConstants.MAX_SEQ_NUM;
            check(packets.get(0).seqNum == expectedSeq, "sequence should wrap modulo 256");
            check(sender.processAck(expectedSeq, KtpConstants.WINDOW_SIZE) == 1,
                    "ACK must remove packet across sequence wraparound");
        }
        check(sender.isAllAcknowledged(), "all wrapped sequence packets should be acknowledged");
        check(sender.getNextSeq() == 1, "next sequence should return to one after 256 messages");
    }

    private static void receiverOrderingAndCapacity() {
        ReceiveWindow receiver = new ReceiveWindow();
        check(receiver.processDataPacket(data(2, 2)) == null, "out-of-order packet must not be ACKed");
        check(receiver.getOutOfOrderCount() == 1, "out-of-order packet should be buffered");
        KtpPacket ack = receiver.processDataPacket(data(1, 1));
        check(ack != null && ack.ackNum == 2, "in-order arrival should drain buffered packet and ACK seq 2");
        check(receiver.getInOrderCount() == 2, "two messages should be queued in order");
        check(receiver.readMessage()[0] == 1, "first application read should be sequence 1");
        check(receiver.readMessage()[0] == 2, "second application read should be sequence 2");

        receiver = new ReceiveWindow();
        for (int i = 1; i <= KtpConstants.WINDOW_SIZE; i++) {
            ack = receiver.processDataPacket(data(i, i));
            check(ack != null && ack.ackNum == i, "each in-order packet should advance cumulative ACK");
        }
        check(receiver.getFreeSpace() == 0, "receive buffer must cap at ten messages");
        ack = receiver.processDataPacket(data(11, 11));
        check(ack != null && ack.rwnd == 0, "full receiver must advertise zero free slots");
        check(receiver.getInOrderCount() == 10, "packet beyond full receive window must not be stored");
        receiver.readMessage();
        check(receiver.getFreeSpace() == 1, "application read must free one complete message slot");
    }
}
