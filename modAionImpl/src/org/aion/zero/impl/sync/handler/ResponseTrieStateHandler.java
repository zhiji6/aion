package org.aion.zero.impl.sync.handler;

import java.util.concurrent.BlockingQueue;
import org.aion.p2p.Ctrl;
import org.aion.p2p.Handler;
import org.aion.p2p.Ver;
import org.aion.zero.impl.sync.Act;
import org.aion.zero.impl.sync.TrieNodeWrapper;
import org.aion.zero.impl.sync.msg.ResponseTrieState;
import org.slf4j.Logger;

/**
 * Handler for trie node responses from the network.
 *
 * @author Alexandra Roatis
 */
public final class ResponseTrieStateHandler extends Handler {

    private final Logger log;

    private final BlockingQueue<TrieNodeWrapper> states;

    /**
     * Constructor.
     *
     * @param log logger for reporting execution information
     * @param states map containing the received states to be processed
     */
    public ResponseTrieStateHandler(final Logger log, final BlockingQueue<TrieNodeWrapper> states) {
        super(Ver.V1, Ctrl.SYNC, Act.RESPONSE_TRIE_STATE);
        this.log = log;
        this.states = states;
    }

    @Override
    public void receive(int peerId, String displayId, final byte[] message) {
        if (message == null || message.length == 0) {
            this.log.debug("<res-trie empty message from peer={}>", displayId);
            return;
        }

        ResponseTrieState response = ResponseTrieState.decode(message);

        if (response != null) {
            if (log.isDebugEnabled()) {
                this.log.debug("<res-trie response={} peer={}>", response, displayId);
            }

            states.add(new TrieNodeWrapper(peerId, displayId, response));
        } else {
            this.log.error("<res-trie decode-error msg-bytes={} peer={}>", message.length, peerId);
        }
    }
}
