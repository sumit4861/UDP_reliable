# Reliable KTP over UDP — Java

This repository implements a message-oriented KTP socket layer over UDP for the assignment **“Emulating End-to-End Reliable Flow Control over Unreliable Communication Channels.”**

## Assignment-aligned protocol constants

- Application payload: at most **512 bytes per message**.
- Receive buffer and maximum window: **10 messages**.
- Sequence numbers: **8-bit**, beginning at sequence 1 and wrapping modulo 256.
- Retransmission timeout: **T = 5 seconds** by default.
- Sender flow control: cumulative ACKs advertise the receiver's available message slots.
- Receiver: buffers valid out-of-order messages, delivers complete messages in order, rejects duplicates/out-of-window packets, and periodically repeats a window update when space becomes available.
- Socket table: up to 10 KTP socket entries.
- Reliability testing: configurable simulated packet-loss probability **p**.

## Requirements

- Java 17 or later (the code uses `ProcessHandle` and NIO).
- Windows: JDK with `java`, `javac`, and `jar` on PATH.
- Optional: GNU Make for the Makefile.

## Build on Windows

From the repository folder:

```bat
build.bat
```

Or compile directly:

```bat
javac -Xlint:all *.java
java ProtocolUnitTests
```

## Run two applications (Windows)

Use **three terminals**, all opened in this repository folder.

1. Start the KTP daemon:

   ```bat
   run_init.bat
   ```

2. Start the receiver first:

   ```bat
   run_user2.bat
   ```

3. Start the sender:

   ```bat
   run_user1.bat
   ```

The default example binds the sender to UDP port 9997 and the receiver to port 8887 on loopback. For command-line use:

```bat
java -cp . User2 output.txt 8887 9997
java -cp . User1 large_input.txt 9997 8887 0.20
```

The two programs must use matching local/remote ports. Use different free port pairs for independent transfers.

## Tests

Compile and run the dependency-free protocol regression tests:

```bat
javac -Xlint:all *.java
java ProtocolUnitTests
```

The regression suite covers packet serialization/checksums, malformed datagrams, the 512-byte limit, the 10-message sender/receiver limits, cumulative ACKs, stale ACK handling, out-of-order delivery, and 8-bit sequence wraparound.

For a file-transfer test, run the receiver and sender in separate terminals and compare the source and destination files after the receiver reports completion. Example PowerShell:

```powershell
Get-FileHash large_input.txt -Algorithm SHA256
Get-FileHash output.txt -Algorithm SHA256
```

The hashes should match exactly.

## Architecture

- `KtpSocket`: public KTP socket API; uses daemon IPC or an embedded engine for single-process tests.
- `InitKSocket`: initializes the shared socket table, Thread R, Thread S, garbage collector, and IPC listener.
- `KtpSharedMemory` / `KtpSocketEntry`: socket table and per-socket state.
- `ThreadR`: UDP receive loop, peer validation, packet checks, ACK/window processing.
- `ThreadS`: send-window scheduling and timeout retransmission.
- `SendWindow` / `ReceiveWindow`: sender and receiver flow-control state.
- `KtpPacket`: packet encoding, decoding, and checksum validation.
- `User1` / `User2`: sample file sender and receiver.
- `EvaluateP` / `TestTransfer`: performance and file-transfer test harnesses.

## Important notes

- The assignment PDF describes a maximum message size of 512 bytes and message-atomic receive semantics. Callers should provide a receive buffer of at least 512 bytes; a too-small buffer must not silently consume/truncate a message.
- The Java daemon is not OS-level shared memory between unrelated JVMs; this project uses a local TCP IPC service to centralize state and coordinate the applications.
- A successful compile or unit-test run does not prove all end-to-end loss scenarios pass. Run file-integrity tests at several loss probabilities and on the target Windows/JDK environment before submission.
