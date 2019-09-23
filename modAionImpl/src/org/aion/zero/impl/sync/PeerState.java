package org.aion.zero.impl.sync;

import static org.aion.p2p.P2pConstant.MAX_REQUEST_SIZE;
import static org.aion.p2p.P2pConstant.SEND_MAX_RATE_TXBC;

import java.util.Arrays;
import java.util.TreeSet;

public final class PeerState {

    public enum Mode {
        /**
         * The peer is in main-chain. Use normal syncing strategy.
         *
         * @implNote When switching to this mode it is not necessary to set the base value. The base
         *     will automatically be set to the current best block.
         */
        NORMAL,

        /** The peer is in side-chain. Sync backward to find the fork point. */
        BACKWARD,

        /** The peer is in side-chain. Sync forward to catch up. */
        FORWARD,

        /**
         * The peer is far ahead of the local chain. Use lightning sync strategy of jumping forward
         * to request blocks out-of-order ahead of import time. Continue by filling the gap to the
         * next jump step.
         */
        LIGHTNING,

        /**
         * The peer was far ahead of the local chain and made a sync jump. Gradually return to
         * normal syncing strategy, allowing time for old lightning sync requests to come in.
         *
         * @implNote When switching to this mode it is not necessary to set the base value. The base
         *     will automatically be set to the current best block.
         */
        THUNDER;

        /**
         * Method for checking if the mode is one of the fast ones, namely {@link Mode#LIGHTNING}
         * and {@link Mode#THUNDER}.
         *
         * @return {@code true} when one of the fast modes, {@code false} otherwise.
         */
        public boolean isFast() {
            return this == Mode.LIGHTNING || this == Mode.THUNDER;
        }
    }

    // Reference to corresponding node
    private int id;
    private String alias;

    // The syncing mode and the base block number
    private Mode mode = Mode.NORMAL;
    private long base;
    private int size = MAX_REQUEST_SIZE;

    // The syncing status
    private long lastBestBlock = 0;
    private TreeSet<Long> headerRequests = new TreeSet<>();

    /** Used by the copy factory method */
    private PeerState() {}

    /** Creates a new peer state. */
    public PeerState(int id, String alias, long lastBestBlock) {
        this.id = id;
        this.alias = alias;
        this.lastBestBlock = lastBestBlock;
    }

    public PeerState newCopy() {
        PeerState newState = new PeerState();
        newState.id = this.id;
        newState.alias = this.alias;
        newState.mode = this.mode;
        newState.base = this.base;
        newState.lastBestBlock = this.lastBestBlock;
        newState.headerRequests.addAll(headerRequests);
        return newState;
    }

    public int getId() {
        return this.id;
    }

    public String getDisplayName() {
        return this.alias;
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public long getLastBestBlock() {
        return lastBestBlock;
    }

    public void setLastBestBlock(long lastBestBlock) {
        this.lastBestBlock = lastBestBlock;
    }

    public long getBase() {
        return base;
    }

    public void setBase(long base) {
        this.base = base;
    }

    /** Stores the nano time of the last header request. */
    public void addHeaderRequest(long latestStatusRequest) {
        headerRequests.add(latestStatusRequest);
    }

    // slightly under the actual limit to avoid issues on the other end
    private static final int MAX_REQUESTS_PER_SECOND = SEND_MAX_RATE_TXBC / 5;
    private static final int ONE_SECOND = 1_000_000_000;

    /** Determines if a request can be sent based on the route cool down. */
    public boolean isAvailable() {
        if (headerRequests.size() < MAX_REQUESTS_PER_SECOND) {
            // have not reached the limit of requests
            return true;
        } else {
            long now = System.nanoTime();
            long first = headerRequests.first();

            if ((now - first) <= ONE_SECOND) {
                // less than a second has passed since the first request
                return false;
            } else {
                // more than 1 second has passed, so we can request again
                // the first request is no longer useful, so we remove it to keep the size capped
                headerRequests.remove(first);
                return true;
            }
        }
    }

    @Override
    public String toString() {
        return "PeerState{" +
            "id=" + id +
            ", alias='" + alias + '\'' +
            ", mode=" + mode +
            ", base=" + base +
            ", size=" + size +
            ", lastBestBlock=" + lastBestBlock +
            ", lastHeaderRequestSize=" + headerRequests.size() +
            '}';
    }
}
