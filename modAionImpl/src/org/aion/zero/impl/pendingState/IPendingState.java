package org.aion.zero.impl.pendingState;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.base.AionTransaction;
import org.aion.base.AionTxReceipt;
import org.aion.mcf.blockchain.Block;
import org.aion.zero.impl.blockchain.AionImpl.TxBroadcastCallback;

public interface IPendingState {
    List<AionTransaction> getPendingTransactions();

    void flushPendingState(Block newBlock, List<AionTxReceipt> receipts);

    void setNewPendingReceiveForMining(AtomicBoolean newPendingTxReceived);

    void setTxBroadcastCallback(TxBroadcastCallback txBroadcastCallback);
}
