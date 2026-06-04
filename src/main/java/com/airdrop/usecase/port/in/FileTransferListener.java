package com.airdrop.usecase.port.in;

import com.airdrop.domain.model.FileTask;

public interface FileTransferListener {
    void onProgressUpdated(FileTask task);
    void onError(FileTask task, String errorMessage);
}
