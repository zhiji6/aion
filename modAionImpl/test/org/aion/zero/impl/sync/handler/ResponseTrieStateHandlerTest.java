package org.aion.zero.impl.sync.handler;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.zero.impl.sync.TrieDatabase.STATE;
import static org.aion.zero.impl.sync.msg.RequestTrieStateTest.altNodeKey;
import static org.aion.zero.impl.sync.msg.RequestTrieStateTest.nodeKey;
import static org.aion.zero.impl.sync.msg.ResponseTrieStateTest.leafValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.aion.rlp.RLP;
import org.aion.zero.impl.sync.TrieNodeWrapper;
import org.aion.zero.impl.sync.msg.ResponseTrieState;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Unit tests for {@link ResponseTrieStateHandler}.
 *
 * @author Alexandra Roatis
 */
public class ResponseTrieStateHandlerTest {
    private static final int peerId = Integer.MAX_VALUE;
    private static final String displayId = "abcdef";

    @Test
    public void testReceive_nullMessage() {
        Logger log = mock(Logger.class);
        BlockingQueue<TrieNodeWrapper> receivedQueue = mock(LinkedBlockingQueue.class);

        ResponseTrieStateHandler handler = new ResponseTrieStateHandler(log, receivedQueue);

        // receive null message
        handler.receive(peerId, displayId, null);

        verify(log, times(1)).debug("<res-trie empty message from peer={}>", displayId);
        verifyZeroInteractions(receivedQueue);
    }

    @Test
    public void testReceive_emptyMessage() {
        Logger log = mock(Logger.class);
        BlockingQueue<TrieNodeWrapper> receivedQueue = mock(LinkedBlockingQueue.class);

        ResponseTrieStateHandler handler = new ResponseTrieStateHandler(log, receivedQueue);

        // receive empty message
        handler.receive(peerId, displayId, new byte[0]);

        verify(log, times(1)).debug("<res-trie empty message from peer={}>", displayId);
        verifyZeroInteractions(receivedQueue);
    }

    @Test
    public void testReceive_incorrectMessage() {
        Logger log = mock(Logger.class);
        BlockingQueue<TrieNodeWrapper> receivedQueue = mock(LinkedBlockingQueue.class);

        ResponseTrieStateHandler handler = new ResponseTrieStateHandler(log, receivedQueue);

        // receive incorrect message
        byte[] outOfOderEncoding =
                RLP.encodeList(
                        RLP.encodeList(
                                RLP.encodeList(
                                        RLP.encodeElement(nodeKey), RLP.encodeElement(leafValue)),
                                RLP.encodeList(
                                        RLP.encodeElement(altNodeKey),
                                        RLP.encodeElement(leafValue))),
                        RLP.encodeElement(nodeKey),
                        RLP.encodeElement(leafValue),
                        RLP.encodeString(STATE.toString()));
        handler.receive(peerId, displayId, outOfOderEncoding);

        verify(log, times(1))
                .error(
                        "<res-trie decode-error msg-bytes={} peer={}>",
                        outOfOderEncoding.length,
                        peerId);
        verifyZeroInteractions(receivedQueue);
    }

    @Test
    public void testReceive_correctMessage() {
        Logger log = mock(Logger.class);
        when(log.isDebugEnabled()).thenReturn(true);

        BlockingQueue<TrieNodeWrapper> receivedQueue = new LinkedBlockingQueue<>();

        ResponseTrieStateHandler handler = new ResponseTrieStateHandler(log, receivedQueue);

        // receive correct message
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(nodeKey),
                        RLP.encodeElement(leafValue),
                        RLP.encodeList(new byte[0]),
                        RLP.encodeString(STATE.toString()));
        handler.receive(peerId, displayId, encoding);

        ResponseTrieState response = ResponseTrieState.decode(encoding);

        verify(log, times(1)).debug("<res-trie response={} peer={}>", response, displayId);

        TrieNodeWrapper node = new TrieNodeWrapper(peerId, displayId, response);
        assertThat(receivedQueue).containsExactly(node);
    }
}
