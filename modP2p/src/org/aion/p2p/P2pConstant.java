package org.aion.p2p;

public class P2pConstant {

    public static final int //
            STOP_CONN_AFTER_FAILED_CONN = 8, //
            FAILED_CONN_RETRY_INTERVAL = 3000, //
            BAN_CONN_RETRY_INTERVAL = 30_000, //
            MAX_BODY_SIZE = 2 * 1024 * 1024 * 32, //
            RECV_BUFFER_SIZE = 8192 * 1024, //
            SEND_BUFFER_SIZE = 8192 * 1024, //

            // max p2p in package capped at 1.
            READ_MAX_RATE = 1,

            // max p2p in package capped for tx broadcast.
            SEND_MAX_RATE_TXBC = 20,
            RECEIVE_MAX_RATE_TXBC = 40,

            // write queue timeout
            WRITE_MSG_TIMEOUT = 5000,

            /**
             * Maximum request size used by LIGHTNING mode peers. Value selected based on the {@link
             * #MIN_REQUEST_SIZE} and {@link #SEND_MAX_RATE_TXBC} according to the algorithm that
             * manages these requests.
             */
            MAX_REQUEST_SIZE = 60, // must be an even number
            /** Request size used by NORMAL mode peers. */
            MIN_REQUEST_SIZE = 24, // must be an even number

            /**
             * Number of requests sent to LIGHTNING peers. Must be an odd number for the computation
             * of the buckets assigned to LIGHTNING requests to not leave gaps.
             */
            STEP_COUNT = 5,

            /**
             * The number of blocks overlapping with the current chain requested at import when the
             * local best block is far from the top block in the peer's chain.
             */
            FAR_OVERLAPPING_BLOCKS = 3, // must be an odd number

            /**
             * The number of blocks overlapping with the current chain requested at import when the
             * local best block is close to the top block in the peer's chain.
             */
            CLOSE_OVERLAPPING_BLOCKS = 15, // must be an odd number

            // NOTE: the 3 values below are interdependent
            // do not change one without considering the impact to the others
            BACKWARD_SYNC_STEP = MIN_REQUEST_SIZE * STEP_COUNT - 1;
}
