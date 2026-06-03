package com.airdrop.usecase.port.out;

import com.airdrop.domain.model.Peer;

public interface FileTransferListener {
    // 發送進度回報
    void onSendStarted(String taskId);
    void onSendProgress(String taskId, long bytesSent, long totalBytes);
    void onSendCompleted(String taskId);
    void onSendFailed(String taskId, Throwable cause);

    // 接收進度回報
    void onReceiveStarted(String taskId, String fileName, long fileSize, Peer sender);
    void onReceiveProgress(String taskId, long bytesReceived, long totalBytes);
    void onReceiveCompleted(String taskId, String savedPath);
    void onReceiveFailed(String taskId, Throwable cause);
}
