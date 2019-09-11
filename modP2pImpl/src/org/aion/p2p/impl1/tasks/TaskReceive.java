package org.aion.p2p.impl1.tasks;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.p2p.Handler;
import org.slf4j.Logger;

public class TaskReceive implements Runnable {

    private final Logger p2pLOG, surveyLog;
    private final AtomicBoolean start;
    private final MsgIn mi;
    private final Map<Integer, List<Handler>> handlers;

    public TaskReceive(
            final Logger p2pLOG,
            final Logger surveyLog,
            final AtomicBoolean _start,
            final MsgIn receivedMsg,
            final Map<Integer, List<Handler>> _handlers) {
        this.p2pLOG = p2pLOG;
        this.surveyLog = surveyLog;
        this.start = _start;
        this.mi = receivedMsg;
        this.handlers = _handlers;
    }

    @Override
    public void run() {
        long startTime, duration;

        try {
            startTime = System.nanoTime();
            List<Handler> hs = this.handlers.get(mi.getRoute());
            if (hs == null) {
                duration = System.nanoTime() - startTime;
                surveyLog.info("TaskReceive: work, duration = {} ns.", duration);
                return;
            }
            for (Handler hlr : hs) {
                if (hlr == null) {
                    duration = System.nanoTime() - startTime;
                    surveyLog.info("TaskReceive: work, duration = {} ns.", duration);
                    continue;
                }

                try {
                    hlr.receive(mi.getNodeId(), mi.getDisplayId(), mi.getMsg());
                } catch (Exception e) {
                    if (p2pLOG.isDebugEnabled()) {
                        p2pLOG.debug("TaskReceive exception.", e);
                    }
                }
            }
            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskReceive: work, duration = {} ns.", duration);
        } catch (Exception e) {
            if (p2pLOG.isDebugEnabled()) {
                p2pLOG.debug("TaskReceive exception.", e);
            }
        }
    }
}
