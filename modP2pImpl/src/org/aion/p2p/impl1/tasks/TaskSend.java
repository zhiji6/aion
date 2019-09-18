package org.aion.p2p.impl1.tasks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.p2p.Header;
import org.aion.p2p.INode;
import org.aion.p2p.INodeMgr;
import org.aion.p2p.IP2pMgr;
import org.aion.p2p.Msg;
import org.aion.p2p.P2pConstant;
import org.aion.p2p.impl.comm.Node;
import org.slf4j.Logger;

public class TaskSend implements Runnable {

    private final Logger p2pLOG, surveyLog;
    private final IP2pMgr mgr;
    private final AtomicBoolean start;
    private final BlockingQueue<MsgOut> sendMsgQue;
    private final INodeMgr nodeMgr;
    private final Selector selector;
    ExecutorService executors =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final Map<Integer, Deque<MsgOut>> pendingRequests = new HashMap<>();
    INode lastNode = new Node(false, new byte[36], new byte[8], 1); // fake node

    public TaskSend(
            final Logger p2pLOG,
            final Logger surveyLog,
            final IP2pMgr _mgr,
            final int _lane,
            final BlockingQueue<MsgOut> _sendMsgQue,
            final AtomicBoolean _start,
            final INodeMgr _nodeMgr,
            final Selector _selector) {

        this.p2pLOG = p2pLOG;
        this.surveyLog = surveyLog;
        this.mgr = _mgr;
        this.sendMsgQue = _sendMsgQue;
        this.start = _start;
        this.nodeMgr = _nodeMgr;
        this.selector = _selector;
    }

    @Override
    public void run() {
        // for runtime survey information
        long startTime, duration;

        while (start.get()) {
            try {
                // process pending requests
                for (Entry<Integer, Deque<MsgOut>> entry : pendingRequests.entrySet()) {
                    Deque<MsgOut> messages = entry.getValue();
                    if (!messages.isEmpty()) {
                        process(messages.pollFirst());
                    }
                }

                startTime = System.nanoTime();
                MsgOut mo = sendMsgQue.take();
                duration = System.nanoTime() - startTime;
                surveyLog.info("TaskSend: wait for msg, duration = {} ns.", duration);
                process(mo);
            } catch (InterruptedException e) {
                p2pLOG.error("task-send-interrupted", e);
                return;
            } catch (RejectedExecutionException e) {
                p2pLOG.warn("task-send-reached thread queue limit", e);
            } catch (Exception e) {
                if (p2pLOG.isDebugEnabled()) {
                    p2pLOG.debug("TaskSend exception.", e);
                }
            }
        }
        executors.shutdown();
    }

    private void process(MsgOut mo) {
        // shouldn't happen; but just in case
        if (mo == null) return;

        // for runtime survey information
        long startTime, duration;

        startTime = System.nanoTime();
        // if timeout , throw away this msg.
        long now = System.currentTimeMillis();
        if (now - mo.getTimestamp() > P2pConstant.WRITE_MSG_TIMEOUT) {
            if (p2pLOG.isDebugEnabled()) {
                p2pLOG.debug("timeout-msg to-node={} timestamp={}", mo.getDisplayId(), now);
            }
            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskSend: timeout, duration = {} ns.", duration);
            return;
        }

        INode node = null;
        switch (mo.getDest()) {
            case ACTIVE:
                node = nodeMgr.getActiveNode(mo.getNodeId());
                break;
            case INBOUND:
                node = nodeMgr.getInboundNode(mo.getNodeId());
                break;
            case OUTBOUND:
                node = nodeMgr.getOutboundNode(mo.getNodeId());
                break;
        }

        if (node != null) {
            if (node.getIdHash() == lastNode.getIdHash()) {
                if (pendingRequests.containsKey(node.getIdHash())) {
                    pendingRequests.get(node.getIdHash()).addLast(mo);
                } else {
                    Deque<MsgOut> dq = new ArrayDeque<>();
                    dq.addLast(mo);
                    pendingRequests.put(node.getIdHash(), dq);
                }
                duration = System.nanoTime() - startTime;
                surveyLog.info("TaskSend: put back, duration = {} ns.", duration);
                return;
            }

            SelectionKey sk = node.getChannel().keyFor(selector);
            if (sk != null) {
                Object attachment = sk.attachment();
                if (attachment != null) {
                    taskWrite(
                            p2pLOG,
                            surveyLog,
                            node.getIdShort(),
                            node.getChannel(),
                            mo.getMsg(),
                            (ChannelBuffer) attachment,
                            this.mgr);
                    lastNode = node;
                }
            }
        } else {
            if (p2pLOG.isDebugEnabled()) {
                p2pLOG.debug("msg-{} ->{} node-not-exist", mo.getDest().name(), mo.getDisplayId());
            }
        }
        duration = System.nanoTime() - startTime;
        surveyLog.info("TaskSend: process message, duration = {} ns.", duration);
    }

    private static final long MAX_BUFFER_WRITE_TIME = 1_000_000_000L;
    private static final long MIN_TRACE_BUFFER_WRITE_TIME = 10_000_000L;

    public void taskWrite(final Logger p2pLOG,
        final Logger surveyLog,
        final String nodeShortId,
        final SocketChannel sc,
        final Msg msg,
        final ChannelBuffer channelBuffer,
        final IP2pMgr p2pMgr) {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

        // for runtime survey information
        long startTime, duration;

        startTime = System.nanoTime();
        // reset allocated buffer and clear messages if the channel is closed
        if (channelBuffer.isClosed()) {
            channelBuffer.refreshHeader();
            channelBuffer.refreshBody();
            p2pMgr.dropActive(channelBuffer.getNodeIdHash(), "close-already");
            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskWrite: check if closed, duration = {} ns.", duration);
            return;
        }
        duration = System.nanoTime() - startTime;
        surveyLog.info("TaskWrite: check if closed, duration = {} ns.", duration);

        long startTime2 = System.nanoTime();
        try {
            startTime = System.nanoTime();
            channelBuffer.lock.lock();

            /*
             * @warning header set len (body len) before header encode
             */
            byte[] bodyBytes = msg.encode();
            int bodyLen = bodyBytes == null ? 0 : bodyBytes.length;
            Header h = msg.getHeader();
            h.setLen(bodyLen);
            byte[] headerBytes = h.encode();

            if (p2pLOG.isTraceEnabled()) {
                p2pLOG.trace(
                    "write id:{} {}-{}-{}",
                    nodeShortId,
                    h.getVer(),
                    h.getCtrl(),
                    h.getAction());
            }

            ByteBuffer buf = ByteBuffer.allocate(headerBytes.length + bodyLen);
            buf.put(headerBytes);
            if (bodyBytes != null) {
                buf.put(bodyBytes);
            }
            buf.flip();
            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskWrite: setup for write, duration = {} ns.", duration);

            long t1 = System.nanoTime(), t2;
            int wrote = 0;
            try {
                startTime = System.nanoTime();
                do {
                    int result = sc.write(buf);
                    wrote += result;

                    if (result == 0) {
                        // @Attention:  very important sleep , otherwise when NIO write buffer full,
                        // without sleep will hangup this thread.
                        Thread.sleep(0, 1);
                    }

                    t2 = System.nanoTime() - t1;
                } while (buf.hasRemaining() && (t2 < MAX_BUFFER_WRITE_TIME));
                duration = System.nanoTime() - startTime;
                surveyLog.info("TaskWrite: write msg {} node={}, duration = {} ns.", msg, nodeShortId, duration);

                if (p2pLOG.isTraceEnabled() && (t2 > MIN_TRACE_BUFFER_WRITE_TIME)) {
                    p2pLOG.trace(
                        "msg write: id {} size {} time {} ms length {}",
                        nodeShortId,
                        wrote,
                        t2,
                        buf.array().length);
                }

            } catch (ClosedChannelException ex1) {
                if (p2pLOG.isDebugEnabled()) {
                    p2pLOG.debug("closed-channel-exception node=" + nodeShortId, ex1);
                }

                channelBuffer.setClosed();
            } catch (IOException ex2) {
                if (p2pLOG.isDebugEnabled()) {
                    p2pLOG.debug(
                        "write-msg-io-exception node="
                            + nodeShortId
                            + " headerBytes="
                            + String.valueOf(headerBytes.length)
                            + " bodyLen="
                            + String.valueOf(bodyLen)
                            + " time="
                            + String.valueOf(System.nanoTime() - t1)
                            + "ns",
                        ex2);
                }

                if (ex2.getMessage().equals("Broken pipe")) {
                    channelBuffer.setClosed();
                }
            }
        } catch (Exception e) {
            p2pLOG.error("TaskWrite exception.", e);
        } finally {
            duration = System.nanoTime() - startTime2;
            surveyLog.info("TaskWrite: start to end of try, duration = {} ns.", duration);
            channelBuffer.lock.unlock();
        }
    }

    // hash mapping channel id to write thread.
    static int hash2Lane(int in) {
        in ^= in >> (32 - 5);
        in ^= in >> (32 - 10);
        in ^= in >> (32 - 15);
        in ^= in >> (32 - 20);
        in ^= in >> (32 - 25);
        return (in & 0b11111);
    }
}
