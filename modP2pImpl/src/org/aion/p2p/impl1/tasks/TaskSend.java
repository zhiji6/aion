package org.aion.p2p.impl1.tasks;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.p2p.INode;
import org.aion.p2p.INodeMgr;
import org.aion.p2p.IP2pMgr;
import org.aion.p2p.P2pConstant;
import org.slf4j.Logger;

public class TaskSend implements Runnable {

    private final Logger p2pLOG, surveyLog;
    private final IP2pMgr mgr;
    private final MsgOut mo;
    private final INodeMgr nodeMgr;
    private final Selector selector;

    public TaskSend(
            final Logger p2pLOG,
            final Logger surveyLog,
            final IP2pMgr _mgr,
            final MsgOut sendMsg,
            final INodeMgr _nodeMgr,
            final Selector _selector) {

        this.p2pLOG = p2pLOG;
        this.surveyLog = surveyLog;
        this.mgr = _mgr;
        this.mo = sendMsg;
        this.nodeMgr = _nodeMgr;
        this.selector = _selector;
    }

    @Override
    public void run() {
        // for runtime survey information
        long startTime, duration;

        try {
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
                SelectionKey sk = node.getChannel().keyFor(selector);
                if (sk != null) {
                    Object attachment = sk.attachment();
                    if (attachment != null) {
                        mgr.executeTpe(
                                new TaskWrite(
                                        p2pLOG,
                                        node.getIdShort(),
                                        node.getChannel(),
                                        mo.getMsg(),
                                        (ChannelBuffer) attachment,
                                        this.mgr));
                    }
                }
            } else {
                if (p2pLOG.isDebugEnabled()) {
                    p2pLOG.debug(
                            "msg-{} ->{} node-not-exist", mo.getDest().name(), mo.getDisplayId());
                }
            }
            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskSend: work, duration = {} ns.", duration);
        } catch (RejectedExecutionException e) {
            p2pLOG.warn("task-send-reached thread queue limit", e);
        } catch (Exception e) {
            if (p2pLOG.isDebugEnabled()) {
                p2pLOG.debug("TaskSend exception.", e);
            }
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
