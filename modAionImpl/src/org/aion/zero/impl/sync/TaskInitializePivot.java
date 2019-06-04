package org.aion.zero.impl.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.aion.p2p.V1Constants;
import org.aion.zero.impl.types.AionBlock;
import org.slf4j.Logger;

/**
 * Requests a block to be used as pivot when the predefined conditions are satisfied.
 *
 * @author Alexandra Roatis
 */
final class TaskInitializePivot implements Runnable {

    private final Logger log;
    private final SyncMgr syncMgr;
    private final FastSyncManager fastSyncMgr;
    private long pivotNumber = -1;
    private Map<Integer, AionBlock> candidatesByPeerId = new HashMap<>();
    private BlockingQueue<BlocksWrapper> pivotCandidates;

    /**
     * Constructor.
     *
     * @param log logger for reporting execution information
     * @param syncMgr manages the sync process and provides necessary information in initializing
     *     the pivot
     */
    TaskInitializePivot(final Logger log, final SyncMgr syncMgr) {
        this.log = log;
        this.syncMgr = syncMgr;
        this.fastSyncMgr = syncMgr.getFastSyncManager();
        this.pivotCandidates = fastSyncMgr.pivotCandidates;
    }

    @Override
    public void run() {
        // initializing the pivot for fast sync should be highest priority
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

        // repeats as long as the pivot is not initialized
        while (fastSyncMgr.getPivot() == null) {
            // ensure minimum number of required peers before initializing the pivot
            if (syncMgr.getActivePeers() < V1Constants.REQUIRED_CONNECTIONS) {
                continue;
            }

            // ensure known network status before initializing the pivot
            if (syncMgr.getNetworkBestBlockNumber() == 0) {
                continue;
            }

            if (pivotNumber == -1) {
                // set number for pivot block
                pivotNumber =
                        syncMgr.getNetworkBestBlockNumber() - V1Constants.PIVOT_DISTANCE_TO_HEAD;

                // ensure that having a pivot makes sense
                if (pivotNumber <= V1Constants.PIVOT_DISTANCE_TO_HEAD) {
                    pivotNumber = -1;
                    // TODO: consider disabling fast sync in this case
                    continue;
                }

                // request pivot blocks from network
                // TODO: ensure that some peer states actually exist
                for (PeerState ps : syncMgr.getPeerStates().values()) {
                    ps.setBaseForPivotRequest(pivotNumber);
                }

                continue;
            } else {
                // already have the target block for pivot
                // take the requested block received most times

                BlocksWrapper tnw;
                try {
                    tnw = fastSyncMgr.pivotCandidates.take();
                    candidatesByPeerId.put(tnw.getNodeIdHash(), tnw.getBlocks().get(0));
                } catch (InterruptedException ex) {
                    if (!fastSyncMgr.isComplete()) {
                        // TODO log
                        log.error("<import-trie-nodes: interrupted without shutdown request>", ex);
                    }
                    return;
                }

                Map<AionBlock, Integer> peersPerBlock =
                        candidatesByPeerId.values().stream()
                                .collect(
                                        Collectors.toMap(
                                                Function.identity(), e -> 1, Math::addExact));
                Optional<Integer> max = peersPerBlock.values().stream().reduce(Math::max);

                // skip selecting pivot until enough candidates are received
                // and consensus condition is fulfilled
                if (!max.isPresent() || !(max.get() >= syncMgr.getActivePeers() * 2 / 3)) {
                    continue;
                } else {
                    fastSyncMgr.setPivot(
                            peersPerBlock.entrySet().stream()
                                    .filter(e -> e.getValue() == max.get())
                                    .collect(Collectors.toList())
                                    // there cannot be multiple options because every peer is
                                    // represented only once in the list
                                    .get(0)
                                    .getKey());
                    fastSyncMgr.pivotCandidates.clear();
                }
            }
        }

        if (log.isDebugEnabled()) {
            // TODO log
            log.debug("<import-trie-nodes: shutdown>");
        }
    }
}
