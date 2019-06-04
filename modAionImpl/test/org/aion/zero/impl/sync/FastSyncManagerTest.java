package org.aion.zero.impl.sync;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.p2p.V1Constants.CONTRACT_MISSING_KEYS_LIMIT;
import static org.aion.zero.impl.sync.DatabaseType.STATE;
import static org.aion.zero.impl.sync.DatabaseType.STORAGE;
import static org.aion.zero.impl.sync.msg.RequestTrieDataTest.altNodeKey;
import static org.aion.zero.impl.sync.msg.RequestTrieDataTest.nodeKey;
import static org.aion.zero.impl.sync.msg.ResponseTrieDataTest.multipleReferences;
import static org.aion.zero.impl.sync.msg.ResponseTrieDataTest.wrappedNodeKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.aion.interfaces.db.InternalVmType;
import org.aion.mcf.core.AccountState;
import org.aion.mcf.valid.BlockHeaderValidator;
import org.aion.p2p.impl1.P2pMgr;
import org.aion.types.Address;
import org.aion.types.ByteArrayWrapper;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.db.AionRepositoryImpl;
import org.aion.zero.impl.db.ContractInformation;
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

    @Test
    public void testIsCompleteContractData_withIncompleteReceipts() {
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);

        FastSyncManager manager =
                spy(
                        new FastSyncManager(
                                chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class)));
        when(manager.isCompleteReceiptData()).thenReturn(false);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isFalse();
    }

    @Test
    public void testIsCompleteContractData_withIncompleteWorldState() {
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);

        FastSyncManager manager =
                spy(
                        new FastSyncManager(
                                chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class)));
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(false);

        assertThat(manager.isCompleteContractData()).isFalse();
    }

    @Test
    public void testIsCompleteContractData_withExistingNodes() {
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        FastSyncManager.Builder builder = new FastSyncManager.Builder();
        FastSyncManager manager =
                builder.withBlockchain(chain).withRequiredStorage(List.of(wrappedNodeKey)).build();

        assertThat(manager.isCompleteContractData()).isFalse();
    }

    @Test
    public void testIsCompleteContractData_withPostPivotInceptionBlock() {

        long pivotBlockNumber = 10;
        Address contract = Address.wrap(nodeKey);

        AionRepositoryImpl repository = mock(AionRepositoryImpl.class);
        // ensure it returns a null account state
        when(repository.getAccountState(contract)).thenReturn(null);

        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        // return one contract address
        when(chain.getContracts()).thenReturn(List.of(contract.toBytes()).iterator());
        // ensure it was created post pivot
        when(chain.getIndexedContractInformation(contract))
                .thenReturn(
                        new ContractInformation(
                                pivotBlockNumber + 1, InternalVmType.UNKNOWN, false));
        // ensure it returns the mock repository
        when(chain.getRepository()).thenReturn(repository);

        FastSyncManager.Builder builder = new FastSyncManager.Builder();
        FastSyncManager manager =
                spy(builder.withBlockchain(chain).withPivotNumber(pivotBlockNumber).build());
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isTrue();
    }

    @Test(expected = IllegalStateException.class)
    public void testIsCompleteContractData_withIllegalState() {
        long pivotBlockNumber = 10;
        Address contract = Address.wrap(nodeKey);

        AionRepositoryImpl repository = mock(AionRepositoryImpl.class);
        // ensure it returns a null account state
        when(repository.getAccountState(contract)).thenReturn(null);

        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        // return one contract address
        when(chain.getContracts()).thenReturn(List.of(contract.toBytes()).iterator());
        // ensure it was created pre pivot
        when(chain.getIndexedContractInformation(contract))
                .thenReturn(
                        new ContractInformation(
                                pivotBlockNumber - 1, InternalVmType.UNKNOWN, false));
        // ensure it returns the mock repository
        when(chain.getRepository()).thenReturn(repository);

        FastSyncManager.Builder builder = new FastSyncManager.Builder();
        FastSyncManager manager =
                spy(builder.withBlockchain(chain).withPivotNumber(pivotBlockNumber).build());
        // ensure it passes the completeness check
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isTrue();
    }

    @Test
    public void testIsCompleteContractData_withCompleteContracts() {
        Address contract1 = Address.wrap(nodeKey);
        Address contract2 = Address.wrap(altNodeKey);

        // mock states to return roots
        AccountState state1 = mock(AccountState.class);
        when(state1.getStateRoot()).thenReturn(nodeKey);
        AccountState state2 = mock(AccountState.class);
        when(state2.getStateRoot()).thenReturn(altNodeKey);

        // mock repository to return account states
        AionRepositoryImpl repository = mock(AionRepositoryImpl.class);
        when(repository.getAccountState(contract1)).thenReturn(state1);
        when(repository.getAccountState(contract2)).thenReturn(state2);

        // mock chain to return contracts and repository
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        when(chain.getContracts())
                .thenReturn(List.of(contract1.toBytes(), contract2.toBytes()).iterator());
        when(chain.getRepository()).thenReturn(repository);

        // mock completeness for the storage
        when(chain.traverseTrieFromNode(nodeKey, STORAGE)).thenReturn(Collections.emptySet());
        when(chain.traverseTrieFromNode(altNodeKey, STORAGE)).thenReturn(Collections.emptySet());

        FastSyncManager manager =
                spy(
                        new FastSyncManager(
                                chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class)));
        // ensure it passes the completeness check
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isTrue();
    }

    @Test
    public void testIsCompleteContractData_withIncompleteContractAndPostPivotInceptionBlock() {
        long pivotBlockNumber = 10;
        Address contract1 = Address.wrap(nodeKey);
        Address contract2 = Address.wrap(altNodeKey);

        // mock states to return roots
        AccountState state2 = mock(AccountState.class);
        when(state2.getStateRoot()).thenReturn(altNodeKey);

        // mock repository to return account states
        AionRepositoryImpl repository = mock(AionRepositoryImpl.class);
        when(repository.getAccountState(contract1)).thenReturn(null);
        when(repository.getAccountState(contract2)).thenReturn(state2);

        // mock chain to return contracts and repository
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        when(chain.getContracts())
                .thenReturn(List.of(contract1.toBytes(), contract2.toBytes()).iterator());
        when(chain.getRepository()).thenReturn(repository);
        // ensure it was created post pivot
        when(chain.getIndexedContractInformation(contract1))
                .thenReturn(
                        new ContractInformation(
                                pivotBlockNumber + 1, InternalVmType.UNKNOWN, false));

        // mock incompleteness for the storage
        when(chain.traverseTrieFromNode(altNodeKey, STORAGE)).thenReturn(references);

        FastSyncManager manager =
                spy(
                        new FastSyncManager(
                                chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class)));
        // ensure it passes the completeness check
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isFalse();
    }

    @Test
    public void testIsCompleteContractData_withIncompleteContracts() {
        Address contract1 = Address.wrap(nodeKey);
        Address contract2 = Address.wrap(altNodeKey);

        // mock states to return roots
        AccountState state1 = mock(AccountState.class);
        when(state1.getStateRoot()).thenReturn(nodeKey);
        AccountState state2 = mock(AccountState.class);
        when(state2.getStateRoot()).thenReturn(altNodeKey);

        // mock repository to return account states
        AionRepositoryImpl repository = mock(AionRepositoryImpl.class);
        when(repository.getAccountState(contract1)).thenReturn(state1);
        when(repository.getAccountState(contract2)).thenReturn(state2);

        // mock chain to return contracts and repository
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        when(chain.getContracts())
                .thenReturn(List.of(contract1.toBytes(), contract2.toBytes()).iterator());
        when(chain.getRepository()).thenReturn(repository);

        // mock incompleteness for the storage
        when(chain.traverseTrieFromNode(nodeKey, STORAGE)).thenReturn(references);
        when(chain.traverseTrieFromNode(altNodeKey, STORAGE)).thenReturn(references);

        FastSyncManager manager =
                spy(
                        new FastSyncManager(
                                chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class)));
        // ensure it passes the completeness check
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isFalse();
    }

    @Test
    public void testIsCompleteContractData_withIncompleteContractsAndLimit() {
        Address contract1 = Address.wrap(nodeKey);
        Address contract2 = Address.wrap(altNodeKey);

        // mock states to return roots
        AccountState state1 = mock(AccountState.class);
        when(state1.getStateRoot()).thenReturn(nodeKey);
        AccountState state2 = mock(AccountState.class);
        when(state2.getStateRoot()).thenReturn(altNodeKey);

        // mock repository to return account states
        AionRepositoryImpl repository = mock(AionRepositoryImpl.class);
        when(repository.getAccountState(contract1)).thenReturn(state1);
        when(repository.getAccountState(contract2)).thenReturn(state2);

        // mock chain to return contracts and repository
        AionBlockchainImpl chain = mock(AionBlockchainImpl.class);
        when(chain.getContracts())
                .thenReturn(List.of(contract1.toBytes(), contract2.toBytes()).iterator());
        when(chain.getRepository()).thenReturn(repository);

        // mock a large first set -> second set will not be required
        Set<ByteArrayWrapper> largeSet = new HashSet<>();
        for (int i = 0; i < CONTRACT_MISSING_KEYS_LIMIT; i++) {
            largeSet.add(mock(ByteArrayWrapper.class));
        }
        when(chain.traverseTrieFromNode(nodeKey, STORAGE)).thenReturn(largeSet);

        FastSyncManager manager =
                spy(
                        new FastSyncManager(
                                chain, mock(BlockHeaderValidator.class), mock(P2pMgr.class)));
        // ensure it passes the completeness check
        when(manager.isCompleteReceiptData()).thenReturn(true);
        when(manager.isCompleteWorldState()).thenReturn(true);

        assertThat(manager.isCompleteContractData()).isFalse();
    }
}
