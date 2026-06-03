package com.airdrop.domain.model;

import java.util.Objects;

/**
 * Peer represents another device on the local network.
 */
public class Peer {
    private String name;
    private String ip;
    private int port; // TCP Port for file transfer
    private long lastSeen; // Timestamp of last received broadcast

    public Peer() {
    }

    public Peer(String name, String ip, int port) {
        this.name = name;
        this.ip = ip;
        this.port = port;
        this.lastSeen = System.currentTimeMillis();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Peer peer = (Peer) o;
        return port == peer.port && Objects.equals(ip, peer.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    @Override
    public String toString() {
        return name + " (" + ip + ":" + port + ")";
    }
}
