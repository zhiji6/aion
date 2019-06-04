package org.aion.zero.impl.sync;

import static org.aion.p2p.V1Constants.BLOCKS_REQUEST_MAXIMUM_BATCH_SIZE;
import static org.aion.p2p.V1Constants.CONTRACT_MISSING_KEYS_LIMIT;
import static org.aion.p2p.V1Constants.TRIE_DATA_REQUEST_MAXIMUM_BATCH_SIZE;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.mcf.core.AccountState;
import org.aion.mcf.valid.BlockHeaderValidator;
import org.aion.p2p.INode;
import org.aion.p2p.impl1.P2pMgr;
import org.aion.types.Address;
import org.aion.types.ByteArrayWrapper;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.db.ContractInformation;
import org.aion.zero.impl.sync.msg.RequestBlocks;
import org.aion.zero.impl.sync.msg.ResponseBlocks;
import org.aion.zero.impl.sync.msg.RequestTrieData;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.types.A0BlockHeader;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;

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
    private final AtomicBoolean completeBlocks = new AtomicBoolean(false);

    private final AionBlockchainImpl chain;
    private final BlockHeaderValidator<A0BlockHeader> blockHeaderValidator;
    private final P2pMgr p2pMgr;

    // TODO: consider adding a FAST_SYNC log as well
    private static final Logger log = AionLoggerFactory.getLogger(LogEnum.SYNC.name());

    private final int QUEUE_LIMIT = 2 * CONTRACT_MISSING_KEYS_LIMIT;

    // states that are required but not yet requested
    private final Queue<ByteArrayWrapper> missingState = new ArrayBlockingQueue<>(QUEUE_LIMIT);
    private final Queue<ByteArrayWrapper> missingStorage = new ArrayBlockingQueue<>(QUEUE_LIMIT);

    // known key-value pairs
    private final Map<ByteArrayWrapper, byte[]> importedState = new ConcurrentHashMap<>();
    private final Map<ByteArrayWrapper, byte[]> importedStorage = new ConcurrentHashMap<>();

    private AionBlock pivot = null;
    private long pivotNumber = -1;

    Map<ByteArrayWrapper, Long> importedBlockHashes =
            Collections.synchronizedMap(new LRUMap<>(4096));
    Map<ByteArrayWrapper, ByteArrayWrapper> receivedBlockHashes =
            Collections.synchronizedMap(new LRUMap<>(1000));

    BlockingQueue<BlocksWrapper> downloadedBlocks = new LinkedBlockingQueue<>();
    Map<ByteArrayWrapper, BlocksWrapper> receivedBlocks = new HashMap<>();

    private final Map<ByteArrayWrapper, byte[]> importedTrieNodes = new ConcurrentHashMap<>();

    public FastSyncManager(
            AionBlockchainImpl chain,
            BlockHeaderValidator<A0BlockHeader> blockHeaderValidator,
            final P2pMgr p2pMgr) {
        this.enabled = true;
        this.chain = chain;
        this.blockHeaderValidator = blockHeaderValidator;
        this.p2pMgr = p2pMgr;
    }

    @VisibleForTesting
    void setPivot(AionBlock pivot) {
        Objects.requireNonNull(pivot);

        this.pivot = pivot;
        this.pivotNumber = pivot.getNumber();
    }

    public AionBlock getPivot() {
        return pivot;
    }

    // TODO: shutdown pool
    ExecutorService executors =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

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
                manager = new FastSyncManager(this.chain, null, null);
            } else {
                manager = new FastSyncManager(null, null, null);
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

    public boolean isCompleteBlockData() {
        if (completeBlocks.get()) {
            // all checks have already passed
            return true;
        } else if (pivot == null) {
            // the pivot was not initialized yet
            return false;
        } else if (chain.getBlockStore().getChainBlockByNumber(1L) == null) {
            // checks for first block for fast fail if incomplete
            return false;
        } else if (chain.findMissingAncestor(pivot) != null) { // long check done last
            // full check from pivot returned block
            // i.e. the chain was incomplete at some point
            return false;
        } else {
            // making the pivot the current best block
            chain.setBestBlock(pivot);

            // walk through the chain to update the total difficulty
            chain.getBlockStore().pruneAndCorrect();
            chain.getBlockStore().flush();

            completeBlocks.set(true);
            return true;
        }
    }

    @VisibleForTesting
    boolean isCompleteReceiptData() {
        // TODO: integrated implementation from separate branch
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
        return isCompleteReceiptData() && isCompleteWorldState();
    }

    @VisibleForTesting
    boolean isCompleteContractData() {
        if (!missingStorage.isEmpty() || !satisfiesContractRequirements()) {
            // checking all contracts is expensive; to efficiently manage memory we do it
            // only if all the already known missing values have been requested
            // and the state and receipts are already complete
            return false;
        } else {
            Iterator<byte[]> iterator = chain.getContracts();

            // check that each contract has all the required data
            while (iterator.hasNext()) {
                Address contract = Address.wrap(iterator.next());
                ContractInformation info = chain.getIndexedContractInformation(contract);
                if (info == null) {
                    // the contracts are returned by the iterator only when they have info
                    // missing info implies some internal storage error
                    // TODO: disable fast sync in method that catches this exception
                    throw new IllegalStateException(
                            "Fast sync encountered an error for which there is no defined recovery path. Disabling fast sync.");
                } else {

                    // determine if the contract was created after the pivot block
                    // (such contracts may be know if a pivot gets updated due to errors)
                    if (info.getInceptionBlock() > pivotNumber) {
                        // post-pivot contracts are not of interest here
                        // TODO: should we delete them?
                        continue;
                    } else {
                        // check for contract information completeness
                        if (!info.isComplete()) {
                            AccountState contractState =
                                    chain.getRepository().getAccountState(contract);

                            if (contractState == null) {
                                // somehow the world state was incorrectly labeled as complete
                                // this should not happen therefore switching off sync
                                // TODO: disable fast sync in method that catches this exception
                                throw new IllegalStateException(
                                        "Fast sync encountered an error for which there is no defined recovery path. Disabling fast sync.");
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
                                    // to efficiently manage memory: stop checking when reaching the
                                    // limit
                                    return false;
                                }
                            }
                            // TODO: merge
                            //                        byte[] root = contractState.getStateRoot();
                            //
                            //                        // traverse trie from root to find missing
                            // nodes
                            //                        Set<ByteArrayWrapper> missing =
                            //                                chain.traverseTrieFromNode(root,
                            // DatabaseType.STORAGE);
                            //
                            //                        missingStorage.addAll(missing);
                            //
                            //                        // TODO: handle details database update
                            //                        if (missing.isEmpty()) {
                            //                            // the storage got completed
                            //                            // TODO: update the contract details and
                            // set info to complete
                            //                        }
                            //
                            //                        if (missingStorage.size() >=
                            // CONTRACT_MISSING_KEYS_LIMIT) {
                            //                            // to efficiently manage memory: stop
                            // checking when reaching the limit
                            //                            return false;
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

    /**
     * Processes a block response by checking the proof-of-work. Adds valid blocks to the import
     * queue.
     *
     * @param peerId the numerical identifier of the peer who sent the response
     * @param displayId the display identifier of the peer who sent the response
     * @param response the response with blocks to be processed
     */
    public void validateAndAddBlocks(int peerId, String displayId, ResponseBlocks response) {
        if (!executors.isShutdown()) {
            executors.submit(
                    new TaskValidateAndAddBlocks(
                            peerId,
                            displayId,
                            response,
                            blockHeaderValidator,
                            downloadedBlocks,
                            importedBlockHashes,
                            receivedBlockHashes,
                            log));
        }
    }

    public void addToImportedBlocks(ByteArrayWrapper hash) {
        this.importedBlockHashes.put(hash, null); // TODO: is there something useful I can add?
        this.receivedBlockHashes.remove(hash);
    }

    public BlocksWrapper takeFilteredBlocks(ByteArrayWrapper requiredHash, long requiredLevel) {
        // first check the map
        if (receivedBlocks.containsKey(requiredHash)) {
            return receivedBlocks.remove(requiredHash);
        } else if (receivedBlockHashes.containsKey(requiredHash)) {
            // retrieve the batch that contains the block
            ByteArrayWrapper wrapperHash = receivedBlockHashes.get(requiredHash);
            return receivedBlocks.remove(wrapperHash);
        }

        // process queue data
        try {
            while (!downloadedBlocks.isEmpty()) {
                BlocksWrapper wrapper = downloadedBlocks.remove();

                if (wrapper != null) {
                    wrapper.getBlocks()
                            .removeIf(b -> importedBlockHashes.containsKey(b.getHashWrapper()));
                    if (!wrapper.getBlocks().isEmpty()) {
                        ByteArrayWrapper firstHash = wrapper.getBlocks().get(0).getHashWrapper();
                        if (firstHash.equals(requiredHash)) {
                            return wrapper;
                        } else {
                            // determine if the block is in the middle of the batch
                            boolean isRequred = false;
                            for (AionBlock block : wrapper.getBlocks()) {
                                ByteArrayWrapper hash = block.getHashWrapper();
                                receivedBlockHashes.put(hash, firstHash);
                                if (hash.equals(requiredHash)) {
                                    isRequred = true;
                                    break;
                                }
                            }
                            if (isRequred) {
                                return wrapper;
                            } else {
                                receivedBlocks.put(firstHash, wrapper);
                            }
                        }
                    }
                }
            }
        } catch (NoSuchElementException e) {
            log.debug("The empty check should have prevented this exception.", e);
        }

        // couldn't find the data, so need to request it
        makeBlockRequests(requiredHash, requiredLevel);

        return null;
    }

    private void makeBlockRequests(ByteArrayWrapper requiredHash, long requiredLevel) {
        // make request for the needed hash
        RequestBlocks request =
                new RequestBlocks(requiredHash.getData(), BLOCKS_REQUEST_MAXIMUM_BATCH_SIZE, true);

        // TODO: improve peer selection
        // TODO: request that level plus further blocks
        INode peer = p2pMgr.getRandom();
        p2pMgr.send(peer.getIdHash(), peer.getIdShort(), request);

        // send an extra request ahead of time
        if (requiredLevel - BLOCKS_REQUEST_MAXIMUM_BATCH_SIZE > 0) {
            peer = p2pMgr.getRandom();
            request =
                    new RequestBlocks(
                            requiredLevel - BLOCKS_REQUEST_MAXIMUM_BATCH_SIZE,
                            BLOCKS_REQUEST_MAXIMUM_BATCH_SIZE,
                            true);

            p2pMgr.send(peer.getIdHash(), peer.getIdShort(), request);
        }
    }
}
