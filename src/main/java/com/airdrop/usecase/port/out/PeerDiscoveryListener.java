package com.airdrop.usecase.port.out;

import com.airdrop.domain.model.Peer;

public interface PeerDiscoveryListener {
    void onPeerDiscovered(Peer peer);
    void onPeerLost(Peer peer);
}