package org.aion.zero.impl.sync;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.aion.p2p.INode;
import org.aion.p2p.IP2pMgr;
import org.aion.zero.impl.sync.msg.RequestTrieData;
import org.slf4j.Logger;

/**
 * Requests the world state and storage from the network.
 *
 * @author Alexandra Roatis
 */
public class TaskGetTrieData implements Runnable {

    private final Logger log;
    private final AtomicBoolean run;

    private final IP2pMgr p2p;
    private final FastSyncManager fastSyncMgr;

    TaskGetTrieData(
            final Logger log,
            final AtomicBoolean run,
            final IP2pMgr p2p,
            final FastSyncManager fastSyncMgr) {
        this.log = log;
        this.run = run;
        this.fastSyncMgr = fastSyncMgr;
        this.p2p = p2p;
    }

    @Override
    public void run() {
        // get all active nodes
        Collection<INode> nodes = this.p2p.getActiveNodes().values();

        // filter nodes by height
        // TODO: add filter based on protocol version to ensure they know the requests
        List<INode> nodesFiltered =
                nodes.stream()
                        .filter(n -> fastSyncMgr.isAbovePivot(n))
                        .collect(Collectors.toList());

        if (nodesFiltered.isEmpty()) {
            return;
        }

        // go though all the available nodes
        for (INode node : nodesFiltered) {
            RequestTrieData request = fastSyncMgr.createNextRequest();

            if (request != null) {
                // send request
                if (log.isDebugEnabled()) {
                    log.debug("<get-headers request={} node={}>", request, node.getIdShort());
                }

                this.p2p.send(node.getIdHash(), node.getIdShort(), request);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("<get-headers no request made>");
                }
                // when the request is null there's nothing left in the list
                break;
            }
        }
    }
}
