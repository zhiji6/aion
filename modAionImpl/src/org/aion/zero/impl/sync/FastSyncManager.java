package org.aion.zero.impl.sync;

import static org.aion.p2p.V1Constants.CONTRACT_MISSING_KEYS_LIMIT;
import static org.aion.p2p.V1Constants.TRIE_DATA_REQUEST_MAXIMUM_BATCH_SIZE;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.base.type.AionAddress;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.mcf.core.AccountState;
import org.aion.p2p.INode;
import org.aion.vm.api.interfaces.Address;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.db.ContractInformation;
import org.aion.zero.impl.sync.msg.RequestTrieData;
import org.aion.zero.impl.types.AionBlock;

/**
 * Directs behavior for fast sync functionality.
 *
 * @author Alexandra Roatis
 */
public final class FastSyncManager {

    // TODO: ensure correct behavior when disabled
    private boolean enabled;
    // TODO: ensure correct behavior when complete
    private final AtomicBoolean complete = new AtomicBoolean(false);

    private final int QUEUE_LIMIT = 2 * CONTRACT_MISSING_KEYS_LIMIT;

    // states that are required but not yet requested
    private final Queue<ByteArrayWrapper> missingState = new ArrayBlockingQueue<>(QUEUE_LIMIT);
    private final Queue<ByteArrayWrapper> missingStorage = new ArrayBlockingQueue<>(QUEUE_LIMIT);

    // known key-value pairs
    private final Map<ByteArrayWrapper, byte[]> importedState = new ConcurrentHashMap<>();
    private final Map<ByteArrayWrapper, byte[]> importedStorage = new ConcurrentHashMap<>();

    private AionBlock pivot = null;
    private long pivotNumber = -1;

    private final AionBlockchainImpl chain;

    public FastSyncManager() {
        this.enabled = false;
        this.chain = null;
    }

    public FastSyncManager(AionBlockchainImpl chain) {
        this.enabled = true;
        this.chain = chain;
    }
    /** This builder allows creating customized {@link FastSyncManager} objects for unit tests. */
    @VisibleForTesting
    static class Builder {
        private AionBlockchainImpl chain = null;
        private Collection<ByteArrayWrapper> storage = null;
        private long pivotNumber = -1;

        public Builder() {}

        public FastSyncManager.Builder withBlockchain(AionBlockchainImpl chain) {
            this.chain = chain;
            return this;
        }

        public FastSyncManager.Builder withRequiredStorage(Collection<ByteArrayWrapper> storage) {
            this.storage = storage;
            return this;
        }

        public FastSyncManager.Builder withPivotNumber(long pivotNumber) {
            this.pivotNumber = pivotNumber;
            return this;
        }

        public FastSyncManager build() {
            FastSyncManager manager;

            if (chain != null) {
                manager = new FastSyncManager(this.chain);
            } else {
                manager = new FastSyncManager();
            }

            // adding required storage
            if (storage != null) {
                manager.missingStorage.addAll(storage);
            }

            // adding pivot number
            if (pivotNumber >= 0) {
                manager.pivotNumber = pivotNumber;
            }

            return manager;
        }
    }

    public void addImportedNode(ByteArrayWrapper key, byte[] value, DatabaseType dbType) {
        if (enabled) {
            switch (dbType) {
                case STATE:
                    importedState.put(key, value);
                    break;
                case STORAGE:
                    importedStorage.put(key, value);
                    break;
                default:
                    break;
            }
        }
    }

    public boolean containsExact(ByteArrayWrapper key, byte[] value, DatabaseType dbType) {
        if (enabled) {
            switch (dbType) {
                case STATE:
                    return importedState.containsKey(key)
                            && Arrays.equals(importedState.get(key), value);
                case STORAGE:
                    return importedStorage.containsKey(key)
                            && Arrays.equals(importedStorage.get(key), value);
                default:
                    return false;
            }

        } else {
            return false;
        }
    }

    // TODO: make provisions for concurrent access
    public RequestTrieData createNextRequest() {
        if (isComplete()) {
            return null;
        }

        // check if any required state entries
        ByteArrayWrapper key = missingState.poll();

        if (key != null) {
            return new RequestTrieData(
                    key.getData(), DatabaseType.STATE, TRIE_DATA_REQUEST_MAXIMUM_BATCH_SIZE);
        }

        // check for required storage
        key = missingStorage.poll();

        if (key != null) {
            return new RequestTrieData(
                    key.getData(), DatabaseType.STORAGE, TRIE_DATA_REQUEST_MAXIMUM_BATCH_SIZE);
        }

        // check/expand world state requirements
        if (!isCompleteWorldState()) {
            key = missingState.poll();

            if (key != null) {
                return new RequestTrieData(
                        key.getData(), DatabaseType.STATE, TRIE_DATA_REQUEST_MAXIMUM_BATCH_SIZE);
            }
        }

        // check/expand storage requirements
        if (!isCompleteContractData()) {
            key = missingStorage.poll();

            if (key != null) {
                return new RequestTrieData(
                        key.getData(), DatabaseType.STORAGE, TRIE_DATA_REQUEST_MAXIMUM_BATCH_SIZE);
            }
        }

        // seems like no data is missing -> check full completeness
        checkCompleteness();

        return null;
    }

    @VisibleForTesting
    void setPivot(AionBlock pivot) {
        Objects.requireNonNull(pivot);

        this.pivot = pivot;
        this.pivotNumber = pivot.getNumber();
    }

    public boolean isAbovePivot(INode n) {
        // TODO: review
        return enabled && n.getBestBlockNumber() > pivotNumber;
    }

    /** Changes the pivot in case of import failure. */
    public void handleFailedImport(
            ByteArrayWrapper key, byte[] value, DatabaseType dbType, int peerId, String peer) {
        if (enabled) {
            // TODO: received incorrect or inconsistent state: change pivot??
            // TODO: consider case where someone purposely sends incorrect values
            // TODO: decide on how far back to move the pivot
        }
    }

    /**
     * Indicates the status of the fast sync process.
     *
     * @return {@code true} when fast sync is complete and secure, {@code false} while trie nodes
     *     are still required or completeness has not been confirmed yet
     */
    public boolean isComplete() {
        return !enabled || complete.get();
    }

    /**
     * Checks that all the conditions for completeness are fulfilled.
     *
     * @implNote Expensive functionality which should not be called frequently.
     */
    private void checkCompleteness() {
        // already complete, do nothing
        if (isComplete()) {
            return;
        }

        // TODO: differentiate between requirements of light clients and full nodes

        // ensure all blocks were received
        if (!isCompleteBlockData()) {
            return;
        }

        // ensure all transaction receipts were received
        if (!isCompleteReceiptData()) {
            return;
        }

        // ensure complete world state for pivot was received
        if (!isCompleteWorldState()) {
            return;
        }

        // ensure complete contract details and storage data was received
        if (!isCompleteContractData()) {
            return;
        }

        // everything is complete
        complete.set(true);
    }

    private boolean isCompleteBlockData() {
        // TODO: block requests should be made backwards from pivot
        // TODO: requests need to be based on hash instead of level
        return false;
    }

    private boolean isCompleteReceiptData() {
        // TODO: integrated implementation from separate branch
        return false;
    }

    @VisibleForTesting
    boolean isCompleteReceiptTransfer() {
        // TODO: implement
        return false;
    }

    @VisibleForTesting
    boolean isCompleteWorldState() {
        if (pivot == null) {
            return false;
        } else {
            // get root of pivot
            byte[] root = pivot.getStateRoot();

            // traverse trie from root to find missing nodes
            Set<ByteArrayWrapper> missing = chain.traverseTrieFromNode(root, DatabaseType.STATE);

            // clearing the queue to ensure we're not still requesting already received nodes
            missingState.clear();

            if (missing.isEmpty()) {
                return true;
            } else {
                missingState.addAll(missing);
                return false;
            }
        }
    }

    /**
     * Check if the receipts have been processed and the world state download is complete.
     *
     * @implNote This condition must pass before checking that the download of <i>details</i> and
     *     <i>storage</i> data for each contract is complete.
     * @return {@code true} when all the receipts have been processed and the world state download
     *     is complete, {@code false} otherwise.
     */
    private boolean satisfiesContractRequirements() {
        // to be sure we have all the contract information we need:
        // 1. to check the receipts for all the deployed contracts
        // 2. to check the state for the root of the details for each contract
        return isCompleteReceiptTransfer() && isCompleteWorldState();
    }

    @VisibleForTesting
    boolean isCompleteContractData() {
        if (!missingStorage.isEmpty() || !satisfiesContractRequirements()) {
            // checking all contracts is expensive; to efficiently manage memory we do this check
            // only if all the already known missing values have been requested
            // and the state and receipts parts are complete
            return false;
        } else {
            Iterator<byte[]> iterator = chain.getContracts();

            // check that each contract has all the required data
            while (iterator.hasNext()) {
                Address contract = AionAddress.wrap(iterator.next());
                ContractInformation info = chain.getIndexedContractInformation(contract);
                if (info == null) {
                    // the contracts are returned by the iterator only when they have info
                    // missing infor implies some internal storage error
                    // TODO: disable fast sync in method that catches this exception
                    throw new IllegalStateException(
                            "Fast sync encountered a error for which there is no defined recovery path. Disabling fast sync.");
                }

                // look only at contracts created pre-pivot and that have not already been completed
                // (additional contracts may be know if a pivot gets updated due to errors)
                if (info.getInceptionBlock() > pivotNumber && !info.isComplete()) {
                    AccountState contractState = chain.getRepository().getAccountState(contract);

                    if (contractState == null) {
                        // somehow the world state was incorrectly labeled as complete
                        // this should not happen so switching off sync
                        // TODO: disable fast sync in method that catches this exception
                        throw new IllegalStateException(
                                "Fast sync encountered a error for which there is no defined recovery path. Disabling fast sync.");
                    } else {
                        byte[] root = contractState.getStateRoot();

                        // traverse trie from root to find missing nodes
                        Set<ByteArrayWrapper> missing =
                                chain.traverseTrieFromNode(root, DatabaseType.STORAGE);

                        missingStorage.addAll(missing);

                        // TODO: handle details database update
                        if (missing.isEmpty()) {
                            // the storage got completed
                            // TODO: update the contract details and set info to complete
                        }

                        if (missingStorage.size() >= CONTRACT_MISSING_KEYS_LIMIT) {
                            // to efficiently manage memory: stop checking when reaching the limit
                            return false;
                        }
                    }
                }
            }

            return missingStorage.isEmpty();
        }
    }

    public void updateRequests(Collection<byte[]> referencedValues, DatabaseType dbType) {
        if (enabled) {
            Set<ByteArrayWrapper> missing = new HashSet<>();
            for (byte[] value : referencedValues) {
                missing.addAll(chain.traverseTrieFromNode(value, DatabaseType.STORAGE));
            }

            switch (dbType) {
                case STATE:
                    missing.removeAll(importedState.keySet());
                    missingState.addAll(missing);
                    break;
                case STORAGE:
                    missing.removeAll(importedStorage.keySet());
                    missingStorage.addAll(missing);
                    break;
                default:
                    break;
            }
        }
    }
}
