package org.aion.zero.impl.sync;

import java.util.Collection;
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

    /** Time to wait after a faild check of pre-conditions. */
    private static final long SLEEP_TIME = 1000;

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

    private void sleep() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            if (fastSyncMgr.isEnabled() && fastSyncMgr.getPivot() == null) {
                // interrupted without requested shutdown
                log.error("<initialize-pivot: interrupted without shutdown request>", e);
            }
            return;
        }
    }

    @Override
    public void run() {
        // initializing the pivot for fast sync should have a high priority
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY - 1);

        // repeats as long as the pivot is not initialized
        while (fastSyncMgr.getPivot() == null) {
            // ensure minimum number of required peers before initializing the pivot
            int peerCount = syncMgr.getActivePeers();
            if (peerCount < V1Constants.REQUIRED_CONNECTIONS) {
                if (log.isDebugEnabled()) {
                    log.debug("<initialize-pivot: current peers={}, waiting for more>", peerCount);
                }
                sleep();
                continue;
            }

            // ensure known network status before initializing the pivot
            long networkBest = syncMgr.getNetworkBestBlockNumber();
            if (networkBest == 0) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "<initialize-pivot: unknown network best, waiting for information>",
                            networkBest);
                }
                sleep();
                continue;
            }

            // ensure that doing as state-transfer sync makes sense
            if (networkBest < V1Constants.MINIMUM_HEIGHT) {
                log.info("<initialize-pivot: disabling fast sync due to short chain>");
                fastSyncMgr.disable();
                return;
            }

            // select number for pivot block
            if (pivotNumber == -1) {
                // the value will be positive due to the MINIMUM_HEIGHT check above
                pivotNumber = networkBest - V1Constants.PIVOT_DISTANCE_TO_HEAD;
                log.info("<initialize-pivot: set pivot number={}>", pivotNumber);

                // TODO: incorrect placement
                // request pivot blocks from each peer on the network
                Collection<PeerState> establishedPeers = syncMgr.getPeerStates().values();
                if (establishedPeers.size() > V1Constants.REQUIRED_CONNECTIONS) {
                    for (PeerState ps : syncMgr.getPeerStates().values()) {
                        ps.setBaseForPivotRequest(pivotNumber);
                    }
                }

                sleep();
                continue;
            } else {
                // already have the target block for pivot
                // take the requested block received most times

                BlocksWrapper tnw;
                try {
                    tnw = fastSyncMgr.pivotCandidates.take();
                    candidatesByPeerId.put(tnw.getNodeIdHash(), tnw.getBlocks().get(0));
                } catch (InterruptedException ex) {
                    if (fastSyncMgr.getPivot() == null) {
                        log.error("<initialize-pivot: interrupted without shutdown request>", ex);
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
            log.debug("<initialize-pivot: shutdown>");
        }
    }
}
