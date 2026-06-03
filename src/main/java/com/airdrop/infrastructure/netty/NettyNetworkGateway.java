package com.airdrop.infrastructure.netty;

import com.airdrop.domain.model.FileTask;
import com.airdrop.domain.model.Peer;
import com.airdrop.usecase.port.in.NetworkGateway;
import com.airdrop.usecase.port.out.PeerDiscoveryListener;
import com.airdrop.usecase.port.out.FileTransferListener;

import java.io.IOException;

public class NettyNetworkGateway implements NetworkGateway {

    private final NettyServer server;
    private final NettyClient client;
    
    private String localPeerName = "AirDrop-Node";

    public NettyNetworkGateway() {
        this.server = new NettyServer();
        this.client = new NettyClient();
    }

    public void setLocalPeerName(String localPeerName) {
        this.localPeerName = localPeerName;
    }

    public int getBoundTcpPort() {
        return server.getBoundTcpPort();
    }

    @Override
    public void startBroadcasting(String localPeerName, int tcpPort) throws IOException {
        this.localPeerName = localPeerName;
        try {
            client.startUdpMulticastPublisher(localPeerName, tcpPort);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start UDP Multicast publisher", e);
        }
    }

    @Override
    public void stopBroadcasting() {
        client.stopUdpPublisher();
    }

    @Override
    public void startListeningForPeers(PeerDiscoveryListener listener) throws IOException {
        try {
            server.startUdpMulticastListener(listener);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start UDP Multicast listener", e);
        }
    }

    @Override
    public void stopListeningForPeers() {
        // Ideally we would close just the UDP channel in NettyServer
        // For simplicity in MVP, we might leave it open or handle in shutdown
    }

    @Override
    public void startServer(int port, FileTransferListener listener) throws IOException {
        try {
            server.startTcpServer(port, listener);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start TCP Server", e);
        }
    }

    @Override
    public void stopServer() {
        server.stop();
    }

    @Override
    public void sendFile(Peer target, FileTask task, FileTransferListener listener) {
        client.sendFile(localPeerName, target, task, listener);
    }

    @Override
    public void cancelTransfer(String taskId) {
        client.cancelTransfer(taskId);
    }

    public void shutdown() {
        server.stop();
        client.stop();
    }
}
