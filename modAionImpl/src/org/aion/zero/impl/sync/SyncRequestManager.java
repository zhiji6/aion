package org.aion.zero.impl.sync;

import static org.aion.p2p.P2pConstant.BACKWARD_SYNC_STEP;
import static org.aion.p2p.P2pConstant.CLOSE_OVERLAPPING_BLOCKS;
import static org.aion.p2p.P2pConstant.FAR_OVERLAPPING_BLOCKS;
import static org.aion.p2p.P2pConstant.MAX_REQUEST_SIZE;
import static org.aion.p2p.P2pConstant.MIN_REQUEST_SIZE;
import static org.aion.zero.impl.sync.PeerState.Mode.BACKWARD;
import static org.aion.zero.impl.sync.PeerState.Mode.FORWARD;
import static org.aion.zero.impl.sync.PeerState.Mode.LIGHTNING;
import static org.aion.zero.impl.sync.PeerState.Mode.NORMAL;
import static org.aion.zero.impl.sync.PeerState.Mode.THUNDER;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.aion.p2p.INode;
import org.aion.p2p.IP2pMgr;
import org.aion.zero.impl.sync.PeerState.Mode;
import org.aion.zero.impl.sync.msg.ReqBlocksHeaders;
import org.aion.zero.impl.sync.statistics.RequestType;
import org.slf4j.Logger;

/**
 * Manages access to the peers and their sync states.
 *
 * @author Alexandra Roatis
 */
public class SyncRequestManager {

    private static final long MAX_DIFF = 1_000;

    // track the different peers
    private final Map<Integer, PeerState> bookedPeerStates, availablePeerStates;

    // store the headers whose bodies have been requested from corresponding peer
    private final Map<Integer, Map<Integer, HeadersWrapper>> storedHeaders;

    private final Set<Integer> knownActiveNodes;

    private long localHeight, networkHeight, requestHeight;

    // external resources
    private final IP2pMgr p2pManager;
    private final SyncStats syncStatistics;
    private final Logger syncLog, surveyLog;

    // TODO: add locking
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public SyncRequestManager(
            IP2pMgr p2pManager, SyncStats syncStatistics, Logger syncLog, Logger surveyLog) {
        // external resources
        this.p2pManager = p2pManager;
        this.syncStatistics = syncStatistics;
        this.syncLog = syncLog;
        this.surveyLog = surveyLog;

        // implementation details
        this.bookedPeerStates = new HashMap<>();
        this.availablePeerStates = new HashMap<>();
        this.storedHeaders = new HashMap<>();
        this.knownActiveNodes = new HashSet<>();
        this.localHeight = 0;
        this.networkHeight = 0;
        this.requestHeight = 0;
    }

    public synchronized void storeHeaders(int idHash, HeadersWrapper headersWrapper) {
        Objects.requireNonNull(headersWrapper);

        // store the received headers for later matching with bodies
        int size = headersWrapper.getHeaders().size();
        if (storedHeaders.containsKey(idHash)) {
            storedHeaders.get(idHash).put(size, headersWrapper);
        } else {
            Map<Integer, HeadersWrapper> peerHeaders = new HashMap<>();
            peerHeaders.put(size, headersWrapper);
            storedHeaders.put(idHash, peerHeaders);
        }

        // headers were received so the peer is available for further requests
        if (bookedPeerStates.containsKey(idHash) && bookedPeerStates.get(idHash).isAvailable()) {
            availablePeerStates.put(idHash, bookedPeerStates.remove(idHash));
        }

        syncLog.info(
                "Headers node={}: {}",
                headersWrapper.getDisplayId(),
                Arrays.toString(storedHeaders.get(idHash).keySet().toArray()));
    }

    public synchronized HeadersWrapper matchHeaders(int idHash, int size) {
        if (!storedHeaders.containsKey(idHash)) {
            return null;
        }

        HeadersWrapper hw = storedHeaders.get(idHash).get(size);
        syncLog.info(
                "Headers node={}: {}",
                hw == null ? null : hw.getDisplayId(),
                Arrays.toString(storedHeaders.get(idHash).keySet().toArray()));

        // these do not get dropped because they will be replaced by a following batch
        // if incorrectly matched to initial bodies, they can still be used
        return hw;
    }

    /**
     * Returns a copy of the active peer states.
     *
     * @return a copy of the active peer states
     */
    public synchronized Map<Integer, PeerState> getPeerStates() {
        Map<Integer, PeerState> allStates = new HashMap<>();
        for (PeerState state : availablePeerStates.values()) {
            allStates.put(state.getId(), state.newCopy());
        }
        for (PeerState state : bookedPeerStates.values()) {
            allStates.put(state.getId(), state.newCopy());
        }
        return allStates;
    }

    public synchronized PeerState getCopy(int idHash) {
        PeerState state = bookedPeerStates.get(idHash);
        if (state == null) {
            state = availablePeerStates.get(idHash);
        }
        return state == null ? null : state.newCopy();
    }

    public synchronized void makeHeadersRequests(long selfNumber, BigInteger selfTd) {
        syncLog.info("Requesting headers.");
        // for runtime survey information
        long startTime = System.nanoTime();

        int count = 0;

        for (PeerState peerState : updateStatesForRequests(selfNumber, selfTd)) {
            String peerAlias = peerState.getDisplayName();

            if (peerState.getBase() <= peerState.getLastBestBlock()
                    || peerState.getLastBestBlock() == 0) {
                // send request
                ReqBlocksHeaders request =
                        new ReqBlocksHeaders(peerState.getBase(), peerState.getSize());
                p2pManager.send(peerState.getId(), peerAlias, request);
                // record that another request has been made
                peerState.addHeaderRequest(System.nanoTime());
                bookedPeerStates.put(peerState.getId(), peerState);

                if (syncLog.isInfoEnabled()) {
                    syncLog.info(
                            "<get-headers mode={} from-num={} size={} node={}>",
                            peerState.getMode(),
                            request.getFromBlock(),
                            request.getTake(),
                            peerAlias);
                }

                // record stats
                syncStatistics.updateTotalRequestsToPeer(peerAlias, RequestType.STATUS);
                syncStatistics.updateRequestTime(peerAlias, System.nanoTime(), RequestType.HEADERS);
                count++;
            } else {
                // make it available for normal requests
                peerState.setBase(selfNumber);
                peerState.setMode(NORMAL);
                availablePeerStates.put(peerState.getId(), peerState);
            }
        }
        // all the available peers have now gotten requests
        availablePeerStates.clear();

        long duration = System.nanoTime() - startTime;
        surveyLog.info(
                "TaskGetHeaders: made {} request{}, duration = {} ns.",
                count,
                (count == 1 ? "" : "s"),
                duration);
    }

    private List<PeerState> updateStatesForRequests(long selfBest, BigInteger selfTd) {
        // make sure peer list is up to date
        updateActiveNodes(selfTd);

        // update the known localHeight
        localHeight = Math.max(localHeight, selfBest);

        long nextFrom;
        Mode nextMode;

        syncLog.info("Available (2): " + Arrays.toString(availablePeerStates.values().toArray()));
        syncLog.info("Booked    (2): " + Arrays.toString(bookedPeerStates.values().toArray()));

        // reset booked states if now available
        if (!bookedPeerStates.isEmpty()) {
            // check if any of the booked states have become available
            Iterator<PeerState> states = bookedPeerStates.values().iterator();
            while (states.hasNext()) {
                PeerState currentState = states.next();
                if (currentState.isAvailable()) {
                    availablePeerStates.put(currentState.getId(), currentState);
                    states.remove();
                }
            }
        }
        syncLog.info("Available (3): " + Arrays.toString(availablePeerStates.values().toArray()));
        syncLog.info("Booked    (3): " + Arrays.toString(bookedPeerStates.values().toArray()));

        // add the requested number to the list of requests to be made
        if (networkHeight >= selfBest + BACKWARD_SYNC_STEP) {
            nextFrom = Math.max(1, selfBest - FAR_OVERLAPPING_BLOCKS);
        } else if (networkHeight >= selfBest - BACKWARD_SYNC_STEP) {
            nextFrom = Math.max(1, selfBest - CLOSE_OVERLAPPING_BLOCKS);
        } else {
            nextFrom = selfBest;
        }
        nextMode = THUNDER;

        List<PeerState> requestStates = new ArrayList<>();
        for (PeerState state : availablePeerStates.values()) {
            // set up the size to decrease the chance of overlap for consecutive headers requests
            // the range is from MIN to MAX_LARGE_REQUEST_SIZE
            // avoids overlap with FAR_OVERLAPPING_BLOCKS and CLOSE_OVERLAPPING_BLOCKS because they
            // are odd and these are even numbers
            int nextSize = state.getSize() - 2;
            if (nextSize < MIN_REQUEST_SIZE) {
                nextSize = MAX_REQUEST_SIZE;
            }

            if (state.getMode() == BACKWARD) {
                state.setBase(Math.max(1, state.getBase() - BACKWARD_SYNC_STEP));
                state.setSize(nextSize);
            } else if (state.getMode() == FORWARD) {
                state.setBase(state.getBase() + state.getSize());
                state.setSize(nextSize);
            } else {
                // if we already made a request from this peer with this base, increase the base
                if (state.getBase() == nextFrom) {
                    nextFrom = nextFrom + state.getSize();
                }

                // under normal circumstances use the predefined request size
                state.setBase(nextFrom);
                state.setMode(nextMode);
                state.setSize(nextSize);

                // update the maximum request height
                requestHeight = Math.max(requestHeight, nextFrom + nextSize);
                // set up for next peer
                nextFrom =
                        requestHeight > selfBest + MAX_DIFF ? nextFrom + nextSize : requestHeight;
                // nextFrom = requestHeight;
                nextMode = LIGHTNING;
            }

            requestStates.add(state);
        }
        availablePeerStates.clear();

        return requestStates;
    }

    /** Ensures the inactive peers are dropped and new peers are added. */
    private void updateActiveNodes(BigInteger selfTd) {
        Map<Integer, INode> current =
                p2pManager.getActiveNodes().values().stream()
                        .filter(n -> isAdequateTotalDifficulty(n, selfTd))
                        .collect(Collectors.toMap(n -> n.getIdHash(), n -> n));

        // remove dropped connections
        Set<Integer> dropped =
                knownActiveNodes.stream()
                        .filter(n -> !current.containsKey(n))
                        .collect(Collectors.toSet());
        for (Integer id : dropped) {
            storedHeaders.remove(id);
            bookedPeerStates.remove(id);
            availablePeerStates.remove(id);
        }

        // add and update peers
        for (INode node : current.values()) {
            Integer id = node.getIdHash();
            if (bookedPeerStates.containsKey(id)) { // update best
                bookedPeerStates.get(id).setLastBestBlock(node.getBestBlockNumber());
            } else if (availablePeerStates.containsKey(id)) { // update best
                availablePeerStates.get(id).setLastBestBlock(node.getBestBlockNumber());
            } else { // add peer
                availablePeerStates.put(
                        id,
                        new PeerState(
                                node.getIdHash(), node.getIdShort(), node.getBestBlockNumber()));
            }

            // update the known network height
            networkHeight = Math.max(networkHeight, node.getBestBlockNumber());
        }
        syncLog.info("Available (1): " + Arrays.toString(availablePeerStates.values().toArray()));
        syncLog.info("Booked    (1): " + Arrays.toString(bookedPeerStates.values().toArray()));

        // update known active nodes
        knownActiveNodes.clear();
        knownActiveNodes.addAll(current.keySet());
    }

    /** Checks that the peer's total difficulty is higher than or equal to the local chain. */
    private boolean isAdequateTotalDifficulty(INode n, BigInteger selfTd) {
        return n.getTotalDifficulty() != null && n.getTotalDifficulty().compareTo(selfTd) >= 0;
    }

    public synchronized void runInMode(int idHash, Mode mode) {
        PeerState state = bookedPeerStates.get(idHash);
        if (state == null) {
            state = availablePeerStates.get(idHash);
        }

        if (state != null) {
            state.setMode(mode);
            if (syncLog.isDebugEnabled()) {
                syncLog.debug(
                        "<import-mode-after: node = {}, sync mode = {}>", idHash, state.getMode());
            }
        }
    }

    public synchronized String dumpPeerStateInfo() {

        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(
                "====================================== sync-peer-states-status ======================================\n");
        sb.append(
                String.format(
                        "   %9s %16s %18s %10s %16s\n",
                        "peer", "# best block", "state", "mode", "base"));
        sb.append(
                "-----------------------------------------------------------------------------------------------------\n");

        for (PeerState s : bookedPeerStates.values()) {
            sb.append(
                    String.format(
                            "   id:%6s %16d %18s %10s %16d\n",
                            s.getDisplayName(),
                            s.getLastBestBlock(),
                            "WAITING",
                            s.getMode(),
                            s.getBase()));
        }

        for (PeerState s : availablePeerStates.values()) {
            sb.append(
                    String.format(
                            "   id:%6s %16d %18s %10s %16d\n",
                            s.getDisplayName(),
                            s.getLastBestBlock(),
                            "AVAILABLE",
                            s.getMode(),
                            s.getBase()));
        }

        return sb.toString();
    }
}
