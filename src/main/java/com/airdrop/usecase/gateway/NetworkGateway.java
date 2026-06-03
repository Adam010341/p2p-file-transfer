package com.airdrop.usecase.gateway;

import com.airdrop.domain.model.Peer;
import com.airdrop.domain.model.FileTask;
import java.io.IOException;

/**
 * NetworkGateway defines the boundary interface for network communication.
 * It is located in the Use Case layer to prevent core logic from depending on external network details (DIP).
 */
public interface NetworkGateway {

    // ==========================================
    // 1. UDP Peer Discovery (UDP 廣播與監聽)
    // ==========================================
    
    /**
     * Start broadcasting the local peer information to the LAN.
     *
     * @param localPeerName The name of this peer (e.g., "Alan's PC").
     * @param tcpPort The TCP port on which this peer is listening for incoming file transfers.
     * @throws IOException If any network errors occur while starting the broadcast.
     */
    void startBroadcasting(String localPeerName, int tcpPort) throws IOException;

    /**
     * Stop broadcasting the local peer information.
     */
    void stopBroadcasting();

    /**
     * Start listening for UDP broadcasts from other peers in the LAN.
     *
     * @param listener Callback listener when a peer is discovered or updated.
     * @throws IOException If any network errors occur while starting the listener.
     */
    void startListeningForPeers(PeerDiscoveryListener listener) throws IOException;

    /**
     * Stop listening for UDP broadcasts.
     */
    void stopListeningForPeers();

    // ==========================================
    // 2. TCP File Transfer (TCP 檔案發送與接收)
    // ==========================================

    /**
     * Start the TCP server to listen for incoming file transfer requests.
     *
     * @param port The port to bind the TCP server.
     * @param listener Callback listener to handle file receive progress and status.
     * @throws IOException If any network errors occur while starting the server.
     */
    void startServer(int port, FileReceiverListener listener) throws IOException;

    /**
     * Stop the TCP server.
     */
    void stopServer();

    /**
     * Send a file to a target peer asynchronously.
     *
     * @param target The target peer to receive the file.
     * @param task The file task containing path, name, size, etc.
     * @param listener Callback listener to handle sending progress and status.
     */
    void sendFile(Peer target, FileTask task, FileTransferListener listener);

    /**
     * Cancel an ongoing file transfer task.
     *
     * @param taskId The unique ID of the file task.
     */
    void cancelTransfer(String taskId);

    // ==========================================
    // 3. Callback Listeners (回呼監聽介面)
    // ==========================================

    interface PeerDiscoveryListener {
        /**
         * Triggered when a peer is discovered or its heartbeat is updated.
         */
        void onPeerDiscovered(Peer peer);
    }

    interface FileReceiverListener {
        /**
         * Triggered when an incoming file transfer request is initiated.
         */
        void onReceiveStarted(String taskId, String fileName, long fileSize, Peer sender);

        /**
         * Triggered periodically to report the file receiving progress.
         */
        void onReceiveProgress(String taskId, long bytesReceived, long totalBytes);

        /**
         * Triggered when the file has been successfully received and saved.
         */
        void onReceiveCompleted(String taskId, String savedPath);

        /**
         * Triggered if the file receiving fails.
         */
        void onReceiveFailed(String taskId, Throwable cause);
    }

    interface FileTransferListener {
        /**
         * Triggered when the file sending task starts.
         */
        void onSendStarted(String taskId);

        /**
         * Triggered periodically to report the file sending progress.
         */
        void onSendProgress(String taskId, long bytesSent, long totalBytes);

        /**
         * Triggered when the file has been successfully sent.
         */
        void onSendCompleted(String taskId);

        /**
         * Triggered if the file sending fails.
         */
        void onSendFailed(String taskId, Throwable cause);
    }
}
