package org.aion.zero.impl.sync;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.zero.impl.sync.DatabaseType.STATE;
import static org.aion.zero.impl.sync.msg.RequestTrieDataTest.nodeKey;
import static org.aion.zero.impl.sync.msg.ResponseTrieDataTest.multipleReferences;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import org.aion.mcf.valid.BlockHeaderValidator;
import org.aion.p2p.impl1.P2pMgr;
import org.aion.types.ByteArrayWrapper;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.types.AionBlock;
import org.junit.Test;

/**
 * Unit tests for {@link FastSyncManager}.
 *
 * @author Alexandra Roatis
 */
public final class FastSyncManagerTest {

    private static Set<ByteArrayWrapper> references = multipleReferences.keySet();

    @Test
    public void testIsCompleteWorldState_withNullPivot() {
        FastSyncManager manager =
                new FastSyncManager(
                        mock(AionBlockchainImpl.class),
                        mock(BlockHeaderValidator.class),
                        mock(P2pMgr.class));

        assertThat(manager.isCompleteWorldState()).isFalse();
    }

    @Test
    public void testIsCompleteWorldState_withNoMissingNodes() {
        byte[] root = nodeKey;

        // return the given key as state root
        AionBlock pivot = mock(AionBlock.class);
        when(pivot.getStateRoot()).thenReturn(root);

        // return an empty set when searching for missing nodes
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        when(chain.traverseTrieFromNode(root, STATE)).thenReturn(Collections.emptySet());

        // set the mock block as pivot
        FastSyncManager manager =
                new FastSyncManager(chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class));
        manager.setPivot(pivot);

        assertThat(manager.isCompleteWorldState()).isTrue();
    }

    @Test
    public void testIsCompleteWorldState_withMissingNodes() {
        byte[] root = nodeKey;

        // return the given key as state root
        AionBlock pivot = mock(AionBlock.class);
        when(pivot.getStateRoot()).thenReturn(root);

        // return a non-empty set when searching for missing nodes
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        when(chain.traverseTrieFromNode(root, STATE)).thenReturn(references);

        // set the mock block as pivot
        FastSyncManager manager =
                new FastSyncManager(chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class));
        manager.setPivot(pivot);

        assertThat(manager.isCompleteWorldState()).isFalse();
    }
}
