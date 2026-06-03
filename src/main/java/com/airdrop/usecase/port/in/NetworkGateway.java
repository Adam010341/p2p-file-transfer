package com.airdrop.usecase.port.in;

import com.airdrop.domain.model.Peer;
import com.airdrop.domain.model.FileTask;
import com.airdrop.usecase.port.out.PeerDiscoveryListener;
import com.airdrop.usecase.port.out.FileTransferListener;
import java.io.IOException;

public interface NetworkGateway {
    void startBroadcasting(String localPeerName, int tcpPort) throws IOException;
    void stopBroadcasting();
    void startListeningForPeers(PeerDiscoveryListener listener) throws IOException;
    void stopListeningForPeers();
    void startServer(int port, FileTransferListener listener) throws IOException;
    void stopServer();
    void sendFile(Peer target, FileTask task, FileTransferListener listener);
    void cancelTransfer(String taskId);
}
