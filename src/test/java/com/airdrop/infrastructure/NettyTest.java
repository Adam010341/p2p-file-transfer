package com.airdrop.infrastructure;

import com.airdrop.domain.model.FileTask;
import com.airdrop.domain.model.Peer;
import com.airdrop.infrastructure.netty.NettyNetworkGateway;
import com.airdrop.usecase.port.out.FileTransferListener;
import com.airdrop.usecase.port.out.PeerDiscoveryListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class NettyTest {

    private NettyNetworkGateway gateway1;
    private NettyNetworkGateway gateway2;

    @BeforeEach
    public void setUp() {
        gateway1 = new NettyNetworkGateway();
        gateway1.setLocalPeerName("Node-1");
        
        gateway2 = new NettyNetworkGateway();
        gateway2.setLocalPeerName("Node-2");
    }

    @AfterEach
    public void tearDown() {
        gateway1.shutdown();
        gateway2.shutdown();
    }

    @Test
    public void testTcpFileTransfer() throws Exception {
        // Create a dummy 1MB file
        File tempFile = File.createTempFile("test_transfer", ".dat");
        tempFile.deleteOnExit();
        byte[] originalData = new byte[1024 * 1024];
        new Random().nextBytes(originalData);
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(originalData);
        }

        CountDownLatch receiveLatch = new CountDownLatch(1);
        AtomicBoolean transferSuccess = new AtomicBoolean(false);

        // Start Receiver (Gateway 2)
        gateway2.startServer(0, new FileTransferListener() {
            @Override
            public void onReceiveStarted(String taskId, String fileName, long fileSize, Peer sender) {
                System.out.println("Node-2 Receive started: " + fileName);
            }

            @Override
            public void onReceiveProgress(String taskId, long bytesReceived, long totalBytes) {}

            @Override
            public void onReceiveCompleted(String taskId, String savedPath) {
                System.out.println("Node-2 Receive completed: " + savedPath);
                try {
                    File receivedFile = new File(savedPath);
                    byte[] receivedData = Files.readAllBytes(receivedFile.toPath());
                    assertArrayEquals(originalData, receivedData, "Received data should exactly match sent data");
                    receivedFile.delete();
                    transferSuccess.set(true);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    receiveLatch.countDown();
                }
            }

            @Override
            public void onReceiveFailed(String taskId, Throwable cause) {
                cause.printStackTrace();
                receiveLatch.countDown();
            }

            // Unused sender methods for receiver
            @Override public void onSendStarted(String taskId) {}
            @Override public void onSendProgress(String taskId, long bytesSent, long totalBytes) {}
            @Override public void onSendCompleted(String taskId) {}
            @Override public void onSendFailed(String taskId, Throwable cause) {}
        });

        int receiverPort = gateway2.getBoundTcpPort();
        Peer target = new Peer("Node-2", "127.0.0.1", receiverPort);
        FileTask task = new FileTask("task-123", tempFile.getAbsolutePath(), tempFile.getName(), tempFile.length(), target);

        CountDownLatch sendLatch = new CountDownLatch(1);

        // Start Sender (Gateway 1)
        gateway1.sendFile(target, task, new FileTransferListener() {
            @Override
            public void onSendStarted(String taskId) {
                System.out.println("Node-1 Send started.");
            }
            @Override
            public void onSendProgress(String taskId, long bytesSent, long totalBytes) {}
            @Override
            public void onSendCompleted(String taskId) {
                System.out.println("Node-1 Send completed.");
                sendLatch.countDown();
            }
            @Override
            public void onSendFailed(String taskId, Throwable cause) {
                cause.printStackTrace();
                sendLatch.countDown();
            }

            // Unused receiver methods for sender
            @Override public void onReceiveStarted(String taskId, String fileName, long fileSize, Peer sender) {}
            @Override public void onReceiveProgress(String taskId, long bytesReceived, long totalBytes) {}
            @Override public void onReceiveCompleted(String taskId, String savedPath) {}
            @Override public void onReceiveFailed(String taskId, Throwable cause) {}
        });

        assertTrue(sendLatch.await(5, TimeUnit.SECONDS), "File sending should complete within 5 seconds");
        assertTrue(receiveLatch.await(5, TimeUnit.SECONDS), "File receiving should complete within 5 seconds");
        assertTrue(transferSuccess.get(), "Transfer should be successful and data integrity verified");
    }

    @Test
    public void testUdpMulticastDiscovery() throws Exception {
        CountDownLatch discoverLatch = new CountDownLatch(1);
        AtomicBoolean discovered = new AtomicBoolean(false);

        // Gateway 2 listens for peers
        gateway2.startListeningForPeers(new PeerDiscoveryListener() {
            @Override
            public void onPeerDiscovered(Peer peer) {
                if ("Node-1".equals(peer.getName())) {
                    System.out.println("Node-2 discovered peer: " + peer.getName() + " on IP " + peer.getIp());
                    discovered.set(true);
                    discoverLatch.countDown();
                }
            }
        });

        // Gateway 1 broadcasts its presence
        gateway1.startBroadcasting("Node-1", 9999);

        // Wait up to 5 seconds for the UDP packet to be received
        // UDP Multicast can be tricky on some local OS network stacks without internet,
        // but it should work if loopback multicast is enabled.
        boolean success = discoverLatch.await(5, TimeUnit.SECONDS);
        
        // We clean up gracefully whether it passed or timed out
        gateway1.stopBroadcasting();
        gateway2.stopListeningForPeers();
        
        // Assert true. If it fails on some local environments (e.g., CI without multicast), 
        // developers may need to mock this or enable local multicast routing.
        assertTrue(success, "Node-2 should have discovered Node-1 via UDP Multicast");
    }
}
