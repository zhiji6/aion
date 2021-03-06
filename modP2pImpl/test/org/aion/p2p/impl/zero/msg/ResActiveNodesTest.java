package org.aion.p2p.impl.zero.msg;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.aion.p2p.Ctrl;
import org.aion.p2p.INode;
import org.aion.p2p.Ver;
import org.aion.p2p.impl.comm.Act;
import org.aion.p2p.impl.comm.Node;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

/** @author chris */
public class ResActiveNodesTest {
    @Mock private Logger p2pLOG;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    private Node randomNode() {
        return new Node(
                ThreadLocalRandom.current().nextBoolean(),
                UUID.randomUUID().toString().getBytes(),
                Node.ipStrToBytes(
                        ThreadLocalRandom.current().nextInt(0, 256)
                                + "."
                                + ThreadLocalRandom.current().nextInt(0, 256)
                                + "."
                                + ThreadLocalRandom.current().nextInt(0, 256)
                                + "."
                                + ThreadLocalRandom.current().nextInt(0, 256)),
                ThreadLocalRandom.current().nextInt());
    }

    @Test
    public void testRoute() {

        ResActiveNodes res = new ResActiveNodes(p2pLOG, new ArrayList<>());
        assertEquals(Ver.V0, res.getHeader().getVer());
        assertEquals(Ctrl.NET, res.getHeader().getCtrl());
        assertEquals(Act.RES_ACTIVE_NODES, res.getHeader().getAction());
    }

    @Test
    public void testEncodeDecode() {

        int m = ThreadLocalRandom.current().nextInt(0, 20);
        List<INode> srcNodes = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            srcNodes.add(randomNode());
        }

        ResActiveNodes res = ResActiveNodes.decode(new ResActiveNodes(p2pLOG, srcNodes).encode(), p2pLOG);
        assertEquals(res.getNodes().size(), m);
        List<INode> tarNodes = res.getNodes();
        for (int i = 0; i < m; i++) {

            INode srcNode = srcNodes.get(i);
            INode tarNode = tarNodes.get(i);

            assertArrayEquals(srcNode.getId(), tarNode.getId());
            assertEquals(srcNode.getIdHash(), tarNode.getIdHash());
            assertArrayEquals(srcNode.getIp(), tarNode.getIp());

            assertEquals(srcNode.getIpStr(), tarNode.getIpStr());
            assertEquals(srcNode.getPort(), tarNode.getPort());
        }
    }

    // Only 40 Active Nodes are returned at MAX
    @Test
    public void testMaxActive() {

        List<INode> srcNodes = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            srcNodes.add(randomNode());
        }

        ResActiveNodes res = ResActiveNodes.decode(new ResActiveNodes(p2pLOG, srcNodes).encode(), p2pLOG);
        assertEquals(40, res.getNodes().size());

        List<INode> tarNodes = res.getNodes();
        for (int i = 0; i < 40; i++) {

            INode srcNode = srcNodes.get(i);
            INode tarNode = tarNodes.get(i);

            assertArrayEquals(srcNode.getId(), tarNode.getId());
            assertEquals(srcNode.getIdHash(), tarNode.getIdHash());
            assertArrayEquals(srcNode.getIp(), tarNode.getIp());

            assertEquals(srcNode.getIpStr(), tarNode.getIpStr());
            assertEquals(srcNode.getPort(), tarNode.getPort());
        }
    }

    @Test
    public void testDecodeNull() {
        assertNull(ResHandshake.decode(null));
        assertNull(ResHandshake.decode(new byte[0]));
    }
}
