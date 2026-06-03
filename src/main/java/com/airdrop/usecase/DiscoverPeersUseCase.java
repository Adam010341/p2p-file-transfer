package com.airdrop.usecase;

import com.airdrop.domain.model.Peer;
import com.airdrop.usecase.port.in.NetworkGateway;
import com.airdrop.usecase.port.out.PeerDiscoveryListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 負責管理區域網路內所有可用節點的生命週期。
 */
public class DiscoverPeersUseCase implements PeerDiscoveryListener {

    private final NetworkGateway networkGateway;
    
    // 記憶體清單：儲存所有存活的節點。
    // 【嚴格警告】必須使用 ConcurrentHashMap，因為 Netty 收到封包的 Thread 與我們踢除節點的 Thread 是不同的！
    private final Map<String, Peer> activePeers = new ConcurrentHashMap<>();
    
    // 背景定時任務：負責擔任「死神」，定期巡邏並踢出斷線的節點
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    // 節點超時門檻：10 秒沒收到廣播，就視為死亡
    private static final long PEER_TIMEOUT_MS = 10000;

    // 依賴注入 (Dependency Injection)：大腦不自己建立網路層，而是由外部 (App.java) 餵進來
    public DiscoverPeersUseCase(NetworkGateway networkGateway) {
        this.networkGateway = networkGateway;
    }

    /**
     * 啟動節點探索與背景巡視機制
     */
    public void start() {
        // 1. 命令網路層開始監聽，並將「自己 (this)」作為回呼的傾聽者傳進去
        networkGateway.startDiscovery(this);

        // 2. 啟動背景巡視任務：延遲 5 秒後開始，每 5 秒執行一次 evictStalePeers()
        scheduler.scheduleAtFixedRate(this::evictStalePeers, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * 關閉探索與釋放資源
     */
    public void stop() {
        networkGateway.stopDiscovery();
        scheduler.shutdownNow();
        activePeers.clear();
    }

    /**
     * 供 UI 層 (Member C) 呼叫，取得目前存活的節點清單
     */
    public List<Peer> getActivePeers() {
        return new ArrayList<>(activePeers.values());
    }

    // =====================================================================
    // 實作 PeerDiscoveryListener 介面 (接收來自 Member A 網路層的回報)
    // =====================================================================

    @Override
    public void onPeerDiscovered(Peer peer) {
        // 使用 IP:Port 作為唯一識別碼
        String peerKey = peer.getIp() + ":" + peer.getPort();
        
        if (activePeers.containsKey(peerKey)) {
            // 老面孔：更新最後看見的時間戳記
            activePeers.get(peerKey).updateLastSeen();
        } else {
            // 新面孔：加入記憶體清單
            activePeers.put(peerKey, peer);
            System.out.println("[Domain 系統通知] 發現新節點: " + peer.getName() + " (" + peer.getIp() + ")");
        }
    }

    @Override
    public void onPeerLost(Peer peer) {
        String peerKey = peer.getIp() + ":" + peer.getPort();
        if (activePeers.remove(peerKey) != null) {
            System.out.println("[Domain 系統通知] 節點主動離線: " + peer.getName());
        }
    }

    // =====================================================================
    // 內部業務邏輯
    // =====================================================================

    /**
     * 背景執行緒：剔除超時未回報的節點
     */
    private void evictStalePeers() {
        long now = System.currentTimeMillis();
        
        // Java 8 的 removeIf 方法，走訪所有節點並進行生死判斷
        activePeers.values().removeIf(peer -> {
            boolean isStale = (now - peer.getLastSeen()) > PEER_TIMEOUT_MS;
            if (isStale) {
                System.out.println("[Domain 系統通知] 節點已超時斷線，從清單剔除: " + peer.getName());
            }
            return isStale;
        });
    }
}