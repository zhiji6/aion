package org.aion.zero.impl;

import static java.lang.Long.max;
import static java.lang.Runtime.getRuntime;
import static java.math.BigInteger.ZERO;
import static java.util.Collections.emptyList;
import static org.aion.mcf.core.ImportResult.EXIST;
import static org.aion.mcf.core.ImportResult.IMPORTED_BEST;
import static org.aion.mcf.core.ImportResult.IMPORTED_NOT_BEST;
import static org.aion.mcf.core.ImportResult.INVALID_BLOCK;
import static org.aion.mcf.core.ImportResult.NO_PARENT;
import static org.aion.util.biginteger.BIUtil.isMoreThan;
import static org.aion.util.conversions.Hex.toHexString;

import java.io.File;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.aion.base.AionTransaction;
import org.aion.crypto.AddressSpecs;
import org.aion.crypto.HashUtil;
import org.aion.crypto.ed25519.ECKeyEd25519;
import org.aion.equihash.EquihashMiner;
import org.aion.evtmgr.IEvent;
import org.aion.evtmgr.IEventMgr;
import org.aion.evtmgr.impl.evt.EventBlock;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.mcf.blockchain.Block;
import org.aion.mcf.blockchain.BlockHeader;
import org.aion.mcf.core.AccountState;
import org.aion.mcf.core.FastImportResult;
import org.aion.mcf.core.ImportResult;
import org.aion.mcf.db.IBlockStoreBase;
import org.aion.mcf.db.Repository;
import org.aion.mcf.db.RepositoryCache;
import org.aion.zero.impl.exceptions.HeaderStructureException;
import org.aion.util.time.TimeUtils;
import org.aion.zero.impl.db.TransactionStore;
import org.aion.mcf.trie.Trie;
import org.aion.mcf.trie.TrieImpl;
import org.aion.mcf.trie.TrieNodeResult;
import org.aion.mcf.types.BlockIdentifier;
import org.aion.zero.impl.types.AbstractBlockHeader.BlockSealType;
import org.aion.mcf.valid.GrandParentBlockHeaderValidator;
import org.aion.mcf.valid.ParentBlockHeaderValidator;
import org.aion.mcf.valid.TransactionTypeRule;
import org.aion.mcf.vm.types.Bloom;
import org.aion.rlp.RLP;
import org.aion.stake.GenesisStakingBlock;
import org.aion.types.AionAddress;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;
import org.aion.util.types.AddressUtils;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.util.types.Hash256;
import org.aion.utils.HeapDumper;
import org.aion.vm.BlockCachingContext;
import org.aion.vm.BulkExecutor;
import org.aion.vm.PostExecutionLogic;
import org.aion.vm.PostExecutionWork;
import org.aion.vm.exception.VMException;
import org.aion.zero.impl.blockchain.ChainConfiguration;
import org.aion.zero.impl.blockchain.StakingContractHelper;
import org.aion.zero.impl.config.CfgAion;
import org.aion.zero.impl.core.IAionBlockchain;
import org.aion.zero.impl.core.energy.AbstractEnergyStrategyLimit;
import org.aion.zero.impl.core.energy.EnergyStrategies;
import org.aion.zero.impl.db.AionBlockStore;
import org.aion.zero.impl.db.AionRepositoryImpl;
import org.aion.zero.impl.sync.DatabaseType;
import org.aion.zero.impl.sync.SyncMgr;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionBlockSummary;
import org.aion.zero.impl.types.AionTxInfo;
import org.aion.zero.impl.types.RetValidPreBlock;
import org.aion.zero.impl.types.StakingBlock;
import org.aion.zero.impl.valid.TXValidator;
import org.aion.zero.impl.valid.TransactionTypeValidator;
import org.aion.zero.impl.types.A0BlockHeader;
import org.aion.mcf.types.AionTxExecSummary;
import org.aion.mcf.types.AionTxReceipt;
import org.aion.zero.impl.types.StakedBlockHeader;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: clean and clarify best block
// bestKnownBlock - block with the highest block number
// pubBestBlock - block with the highest total difficulty
// bestBlock - current best block inside the blockchain implementation

/**
 * Core blockchain consensus algorithms, the rule within this class decide whether the correct chain
 * from branches and dictates the placement of items into {@link AionRepositoryImpl} as well as
 * managing the state trie. This module also collects stats about block propagation, from its point
 * of view.
 *
 * <p>The module is also responsible for generate new blocks, mostly called by {@link EquihashMiner}
 * to generate new blocks to mine. As for receiving blocks, this class interacts with {@link
 * SyncMgr} to manage the importing of blocks from network.
 */
public class AionBlockchainImpl implements IAionBlockchain {

    private static final Logger LOG = LoggerFactory.getLogger(LogEnum.CONS.name());
    private static final Logger TX_LOG = LoggerFactory.getLogger(LogEnum.TX.name());
    private static final int DIFFICULTY_BYTES = 16;
    private static final Logger LOGGER_VM = AionLoggerFactory.getLogger(LogEnum.VM.toString());
    static long fork040BlockNumber = -1L;
    private static long FORK_5_BLOCK_NUMBER = Long.MAX_VALUE;
    private static boolean fork040Enable;

    private final GrandParentBlockHeaderValidator preUnityGrandParentBlockHeaderValidator;
    private final GrandParentBlockHeaderValidator unityGrandParentBlockHeaderValidator;
    private final ParentBlockHeaderValidator chainParentBlockHeaderValidator;
    private final ParentBlockHeaderValidator sealParentBlockHeaderValidator;
    private final StakingContractHelper stakingContractHelper;
    /**
     * Chain configuration class, because chain configuration may change dependant on the block
     * being executed. This is simple for now but in the future we may have to create a "chain
     * configuration provider" to provide us different configurations.
     */
    ChainConfiguration chainConfiguration;

    private A0BCConfig config;
    private long exitOn = Long.MAX_VALUE;
    private AionRepositoryImpl repository;
    private RepositoryCache<AccountState, IBlockStoreBase> track;
    private TransactionStore transactionStore;
    private Block bestBlock;

    private StakingBlock bestStakingBlock;
    private AionBlock bestMiningBlock;

    /**
     * This version of the bestBlock is only used for external reference (ex. through {@link
     * #getBestBlock()}), this is done because {@link #bestBlock} can slip into temporarily
     * inconsistent states while forking, and we don't want to expose that information to external
     * actors.
     *
     * <p>However we would still like to publish a bestBlock without locking, therefore we introduce
     * a volatile block that is only published when all forking/appending behaviour is completed.
     */
    private volatile Block pubBestBlock;


    class UnityDifficulty {
        private final BigInteger totalDifficulty;
        private final BigInteger totalMiningDifficulty;
        private final BigInteger totalStakingDifficulty;

        UnityDifficulty() {
            totalDifficulty = ZERO;
            totalMiningDifficulty = ZERO;
            totalStakingDifficulty = ZERO;
        }

        UnityDifficulty(UnityDifficulty ud) {
            totalDifficulty = ud.totalDifficulty;
            totalMiningDifficulty = ud.totalMiningDifficulty;
            totalStakingDifficulty = ud.totalStakingDifficulty;
        }

        UnityDifficulty(BigInteger td, BigInteger tmd, BigInteger tsd) {
            totalDifficulty = td;
            totalMiningDifficulty = tmd;
            totalStakingDifficulty = tsd;
        }
    }

    /** use AtomicReference to make sure the difficulty update at the same time */
    private AtomicReference<UnityDifficulty> unityDifficulty = new AtomicReference<>(new UnityDifficulty());


    private ChainStatistics chainStats;
    private AtomicReference<BlockIdentifier> bestKnownBlock = new AtomicReference<>();
    private boolean fork = false;
    private AionAddress minerCoinbase;
    private AionAddress stakerCoinbase;
    private byte[] minerExtraData;
    private Stack<State> stateStack = new Stack<>();
    private IEventMgr evtMgr = null;
    private AbstractEnergyStrategyLimit energyLimitStrategy;
    private AtomicLong bestBlockNumber = new AtomicLong(0L);

    // fields used to manage AVM caching
    // TODO: if refactoring the add(Block) method, these should be used as parameters
    BlockCachingContext executionTypeForAVM = BlockCachingContext.MAINCHAIN;
    long cachedBlockNumberForAVM = 0L;
    private static final long NO_FORK_LEVEL = -1L;
    private long forkLevel = NO_FORK_LEVEL;

    private final boolean storeInternalTransactions;
    //TODO : [unity] find the proper number for chaching the template.
    private Map<ByteArrayWrapper, StakingBlock> stakingBlockTemplate = Collections
        .synchronizedMap(new LRUMap<>(64));

    private final SortedMap<Long, LinkedHashSet<StakingBlock>> sealednewStakingBlock =
        Collections.synchronizedSortedMap(new TreeMap<>());

    private AionBlockchainImpl() {
        this(generateBCConfig(CfgAion.inst()), AionRepositoryImpl.inst(), new ChainConfiguration());
    }

    protected AionBlockchainImpl(
            final A0BCConfig config,
            final AionRepositoryImpl repository,
            final ChainConfiguration chainConfig) {

        // TODO AKI-318: this specialized class is very cumbersome to maintain; could be replaced with CfgAion
        this.config = config;
        this.repository = repository;
        this.chainStats = new ChainStatistics();
        this.storeInternalTransactions = config.isInternalTransactionStorageEnabled();

        /**
         * Because we dont have any hardforks, later on chain configuration must be determined by
         * blockHash and number.
         */
        this.chainConfiguration = chainConfig;

        sealParentBlockHeaderValidator = chainConfiguration.createSealParentBlockHeaderValidator();
        chainParentBlockHeaderValidator = chainConfig.createChainParentBlockHeaderValidator();

        preUnityGrandParentBlockHeaderValidator = chainConfiguration.createPreUnityGrandParentHeaderValidator();
        unityGrandParentBlockHeaderValidator = chainConfiguration.createUnityGrandParentHeaderValidator();


        this.transactionStore = this.repository.getTransactionStore();

        this.minerCoinbase = this.config.getMinerCoinbase();
        if (minerCoinbase == null) {
            LOG.warn("No miner Coinbase!");
        }

        stakerCoinbase = config.getStakerCoinbase();
        if (stakerCoinbase == null && config.isInternalStakingEnabled()) {
            LOG.warn("No staker Coinbase!");
            throw new IllegalStateException("The staker coinbase address is required for the internal staker.");
        }

        /** Save a copy of the miner extra data */
        byte[] extraBytes = this.config.getExtraData();
        this.minerExtraData =
                new byte[this.chainConfiguration.getConstants().getMaximumExtraDataSize()];
        if (extraBytes.length < this.chainConfiguration.getConstants().getMaximumExtraDataSize()) {
            System.arraycopy(extraBytes, 0, this.minerExtraData, 0, extraBytes.length);
        } else {
            System.arraycopy(
                    extraBytes,
                    0,
                    this.minerExtraData,
                    0,
                    this.chainConfiguration.getConstants().getMaximumExtraDataSize());
        }
        this.energyLimitStrategy = config.getEnergyLimitStrategy();

        stakingContractHelper =
                new StakingContractHelper(ChainConfiguration.getStakingContractAddress(), this);
    }

    /**
     * Helper method for generating the adapter between this class and {@link CfgAion}
     *
     * @param cfgAion
     * @return {@code configuration} instance that directly references the singleton instance of
     *     cfgAion
     */
    private static A0BCConfig generateBCConfig(CfgAion cfgAion) {

        Long blkNum = monetaryUpdateBlkNum(cfgAion.getFork().getProperties());

        if (blkNum != null) {
            fork040BlockNumber = blkNum;
        }

        Long unityForkNumber = unityUpdateBlkNum(cfgAion.getFork().getProperties());
        if (unityForkNumber != null) {
            LOG.info("Unity enabled at fork number " + unityForkNumber);
            FORK_5_BLOCK_NUMBER = unityForkNumber;
        }

        BigInteger initialSupply = ZERO;
        for (AccountState as : cfgAion.getGenesis().getPremine().values()) {
            initialSupply = initialSupply.add(as.getBalance());
        }

        ChainConfiguration config = new ChainConfiguration(blkNum, initialSupply);
        return new A0BCConfig() {
            @Override
            public AionAddress getCoinbase() {
                return cfgAion.getGenesis().getCoinbase();
            }

            @Override
            public byte[] getExtraData() {
                return cfgAion.getConsensus().getExtraData().getBytes();
            }

            @Override
            public boolean getExitOnBlockConflict() {
                return true;
                // return cfgAion.getSync().getExitOnBlockConflict();
            }

            @Override
            public AionAddress getMinerCoinbase() {
                return AddressUtils.wrapAddress(cfgAion.getConsensus().getMinerAddress());
            }

            @Override
            public AionAddress getStakerCoinbase() {
                if (cfgAion.getConsensus().getStakerCoinbase() == null) {
                    return null;
                }

                return AddressUtils.wrapAddress(cfgAion.getConsensus().getStakerCoinbase());
            }

            // TODO: hook up to configuration file
            @Override
            public int getFlushInterval() {
                return 1;
            }

            @Override
            public AbstractEnergyStrategyLimit getEnergyLimitStrategy() {
                return EnergyStrategies.getEnergyStrategy(
                        cfgAion.getConsensus().getEnergyStrategy().getStrategy(),
                        cfgAion.getConsensus().getEnergyStrategy(),
                        config);
            }

            @Override
            public boolean isInternalTransactionStorageEnabled() {
                return CfgAion.inst().getDb().isInternalTxStorageEnabled();
            }

            @Override
            public boolean isInternalStakingEnabled() {
                return CfgAion.inst().getConsensus().getStaking();
            }
        };
    }

    private static Long monetaryUpdateBlkNum(Properties properties) {
        if (properties == null) {
            return null;
        }

        String monetaryForkNum = properties.getProperty("fork0.4.0");
        return monetaryForkNum == null ? null : Long.valueOf(monetaryForkNum);
    }

    private static Long unityUpdateBlkNum(Properties properties) {
        if (properties == null) {
            return null;
        }

        String unityForkNum = properties.getProperty("fork0.5.0");
        return unityForkNum == null ? null : Long.valueOf(unityForkNum);
    }

    public static AionBlockchainImpl inst() {
        return Holder.INSTANCE;
    }

    private static byte[] calcTxTrie(List<AionTransaction> transactions) {

        if (transactions == null || transactions.isEmpty()) {
            return HashUtil.EMPTY_TRIE_HASH;
        }

        Trie txsState = new TrieImpl(null);

        for (int i = 0; i < transactions.size(); i++) {
            byte[] txEncoding = transactions.get(i).getEncoded();
            if (txEncoding != null) {
                txsState.update(RLP.encodeInt(i), txEncoding);
            } else {
                return HashUtil.EMPTY_TRIE_HASH;
            }
        }
        return txsState.getRootHash();
    }

    private static byte[] calcReceiptsTrie(List<AionTxReceipt> receipts) {
        if (receipts == null || receipts.isEmpty()) {
            return HashUtil.EMPTY_TRIE_HASH;
        }

        Trie receiptsTrie = new TrieImpl(null);
        for (int i = 0; i < receipts.size(); i++) {
            receiptsTrie.update(RLP.encodeInt(i), receipts.get(i).getReceiptTrieEncoded());
        }
        return receiptsTrie.getRootHash();
    }

    private static byte[] calcLogBloom(List<AionTxReceipt> receipts) {
        if (receipts == null || receipts.isEmpty()) {
            return new byte[Bloom.SIZE];
        }

        Bloom retBloomFilter = new Bloom();
        for (AionTxReceipt receipt : receipts) {
            retBloomFilter.or(receipt.getBloomFilter());
        }

        return retBloomFilter.getBloomFilterBytes();
    }

    /**
     * Returns a {@link PostExecutionWork} object whose {@code doWork()} method will run the
     * provided logic defined in this method. This work is to be applied after each transaction has
     * been run.
     *
     * <p>This "work" is specific to the {@link AionBlockchainImpl#generatePreBlock(Block)}
     * method.
     */
    private static PostExecutionWork getPostExecutionWorkForGeneratePreBlock(
            Repository repository) {
        PostExecutionLogic logic =
                (topRepository, childRepository, transactionSummary, transaction) -> {
                    if (!transactionSummary.isRejected()) {
                        childRepository.flush();

                        AionTxReceipt receipt = transactionSummary.getReceipt();
                        receipt.setPostTxState(topRepository.getRoot());
                        receipt.setTransaction(transaction);
                    }
                };

        return new PostExecutionWork(repository, logic);
    }

    /**
     * Returns a {@link PostExecutionWork} object whose {@code doWork()} method will run the
     * provided logic defined in this method. This work is to be applied after each transaction has
     * been run.
     *
     * <p>This "work" is specific to the {@link AionBlockchainImpl#applyBlock(Block)} method.
     */
    private static PostExecutionWork getPostExecutionWorkForApplyBlock(Repository repository) {
        PostExecutionLogic logic =
                (topRepository, childRepository, transactionSummary, transaction) -> {
                    childRepository.flush();
                    AionTxReceipt receipt = transactionSummary.getReceipt();
                    receipt.setPostTxState(topRepository.getRoot());
                };

        return new PostExecutionWork(repository, logic);
    }

    private static boolean checkFork040(long blkNum) {
        if (fork040BlockNumber != -1) {
            return blkNum >= fork040BlockNumber;
        } else {
            return false;
        }
    }

    /**
     * Should be set after initialization, note that the blockchain will still operate if not set,
     * just will not emit events.
     *
     * @param eventManager
     */
    public void setEventManager(IEventMgr eventManager) {
        this.evtMgr = eventManager;
    }

    public AionBlockStore getBlockStore() {
        return repository.getBlockStore();
    }

    /**
     * Referenced only by external
     *
     * <p>Note: If you are making changes to this method and want to use it to track internal state,
     * use {@link #bestBlock} instead
     *
     * @return {@code bestAionBlock}
     * @see #pubBestBlock
     */
    public byte[] getBestBlockHash() {
        return getBestBlock().getHash();
    }

    /**
     * Referenced only by external
     *
     * <p>Note: If you are making changes to this method and want to use it to track internal state,
     * opt for {@link #getSizeInternal()} instead.
     *
     * @return {@code positive long} representing the current size
     * @see #pubBestBlock
     */
    public long getSize() {
        return getBestBlock().getNumber() + 1;
    }

    /** @see #getSize() */
    private long getSizeInternal() {
        return this.bestBlock.getNumber() + 1;
    }

    public Block getBlockByNumber(long blockNr) {
        return getBlockStore().getChainBlockByNumber(blockNr);
    }

    public List<Block> getBlocksByRange(long first, long last) {
        return getBlockStore().getBlocksByRange(first, last);
    }

    /* NOTE: only returns receipts from the main chain */
    public AionTxInfo getTransactionInfo(byte[] hash) {

        List<AionTxInfo> infos = transactionStore.get(hash);

        if (infos == null || infos.isEmpty()) {
            return null;
        }

        AionTxInfo txInfo = null;
        if (infos.size() == 1) {
            txInfo = infos.get(0);
        } else {
            // pick up the receipt from the block on the main chain
            for (AionTxInfo info : infos) {
                Block block = getBlockStore().getBlockByHash(info.getBlockHash());
                if (block == null) continue;

                Block mainBlock = getBlockStore().getChainBlockByNumber(block.getNumber());
                if (mainBlock == null) continue;

                if (Arrays.equals(info.getBlockHash(), mainBlock.getHash())) {
                    txInfo = info;
                    break;
                }
            }
        }
        if (txInfo == null) {
            LOG.warn("Can't find block from main chain for transaction " + toHexString(hash));
            return null;
        }

        AionTransaction tx =
                this.getBlockByHash(txInfo.getBlockHash())
                        .getTransactionsList()
                        .get(txInfo.getIndex());
        txInfo.setTransaction(tx);
        return txInfo;
    }

    // returns transaction info (tx receipt) without the transaction embedded in it.
    // saves on db reads for api when processing large transactions
    public AionTxInfo getTransactionInfoLite(byte[] txHash, byte[] blockHash) {
        return transactionStore.get(txHash, blockHash);
    }

    public Block getBlockByHash(byte[] hash) {
        return getBlockStore().getBlockByHash(hash);
    }

    public List<byte[]> getListOfHashesEndWith(byte[] hash, int qty) {
        return getBlockStore().getListHashesEndWith(hash, qty < 1 ? 1 : qty);
    }

    public List<byte[]> getListOfHashesStartFromBlock(long blockNumber, int qty) {
        // avoiding errors due to negative qty
        qty = qty < 1 ? 1 : qty;

        long bestNumber = bestBlock.getNumber();

        if (blockNumber > bestNumber) {
            return emptyList();
        }

        if (blockNumber + qty - 1 > bestNumber) {
            qty = (int) (bestNumber - blockNumber + 1);
        }

        long endNumber = blockNumber + qty - 1;

        Block block = getBlockByNumber(endNumber);

        List<byte[]> hashes = getBlockStore().getListHashesEndWith(block.getHash(), qty);

        // asc order of hashes is required in the response
        Collections.reverse(hashes);

        return hashes;
    }

    public AionRepositoryImpl getRepository() {
        return repository;
    }

    public void setRepository(AionRepositoryImpl repository) {
        this.repository = repository;
    }

    private State pushState(byte[] bestBlockHash) {
        State push = stateStack.push(new State());
        this.bestBlock = getBlockStore().getBlockByHashWithInfo(bestBlockHash);

        if (bestBlock instanceof AionBlock) {
            bestMiningBlock = (AionBlock) bestBlock;
            bestStakingBlock = (StakingBlock) getBlockStore().getBlockByHash(bestBlock.getAntiparentHash());
        } else if (bestBlock instanceof StakingBlock) {
            bestStakingBlock = (StakingBlock) bestBlock;
            bestMiningBlock = (AionBlock) getBlockStore().getBlockByHash(bestBlock.getAntiparentHash());
        } else {
            throw new IllegalStateException("Invalid best block data!");
        }

        unityDifficulty.set(
                new UnityDifficulty(
                        bestBlock.getCumulativeDifficulty(),
                        bestBlock.getMiningDifficulty(),
                        bestBlock.getStakingDifficulty()));

        this.repository =
                (AionRepositoryImpl) this.repository.getSnapshotTo(this.bestBlock.getStateRoot());
        return push;
    }

    private void popState() {
        State state = stateStack.pop();
        this.repository = state.savedRepo;
        this.bestBlock = state.savedBest;

        bestMiningBlock = state.savedBestMining;
        bestStakingBlock = state.savedBestStaking;

        unityDifficulty.set(state.ud);
    }

    private void dropState() {
        stateStack.pop();
    }

    /**
     * Not thread safe, currently only run in {@link #tryToConnect(Block)}, assumes that the
     * environment is already locked
     *
     * @param block
     * @return
     */
    private AionBlockSummary tryConnectAndFork(final Block block) {
        State savedState = pushState(block.getParentHash());
        this.fork = true;

        AionBlockSummary summary = null;
        try {
            summary = add(block);
        } catch (Exception e) {
            LOG.error("Unexpected error: ", e);
        } finally {
            this.fork = false;
        }

        if (summary != null && isMoreThan(unityDifficulty.get().totalDifficulty, savedState.ud.totalDifficulty)) {

            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "branching: from = {}/{}, to = {}/{}",
                        savedState.savedBest.getNumber(),
                        toHexString(savedState.savedBest.getHash()),
                        block.getNumber(),
                        toHexString(block.getHash()));
            }

            // main branch become this branch cause we proved that total difficulty is greater
            forkLevel = getBlockStore().reBranch(block);

            // The main repository rebranch
            this.repository = savedState.savedRepo;
            this.repository.syncToRoot(block.getStateRoot());

            // flushing
            flush();

            dropState();
        } else {
            // Stay on previous branch
            popState();
        }

        return summary;
    }

    /**
     * Heuristic for skipping the call to tryToConnect with very large or very small block number.
     */
    public boolean skipTryToConnect(long blockNumber) {
        long current = bestBlockNumber.get();
        return blockNumber > current + 32 || blockNumber < current - 32;
    }

    public byte[] getTrieNode(byte[] key, DatabaseType dbType) {
        return repository.getTrieNode(key, dbType);
    }

    public Map<ByteArrayWrapper, byte[]> getReferencedTrieNodes(
            byte[] value, int limit, DatabaseType dbType) {
        return repository.getReferencedTrieNodes(value, limit, dbType);
    }

    public StakingContractHelper getStakingContractHelper() {
        return stakingContractHelper;
    }

    public StakingBlock getBestStakingBlock() {
        return bestStakingBlock;
    }

    public AionBlock getBestMiningBlock() {
        return bestMiningBlock;
    }

    public void setBestStakingBlock(StakingBlock block) {
        if (block == null) {
            throw new NullPointerException();
        }
        bestStakingBlock = block;
    }

    public void setBestMiningBlock(AionBlock block) {
        if (block == null) {
            throw new NullPointerException();
        }
        bestMiningBlock = block;
    }

    //TODO : [unity] redesign the blockstore datastucture can read the staking/mining block directly.
    public void loadBestMiningBlock() {
        if (bestBlock.getHeader().getSealType() == BlockSealType.SEAL_POW_BLOCK.getSealId()) {
            bestMiningBlock = (AionBlock) bestBlock;
        } else if (bestBlock.getHeader().getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {
            bestMiningBlock = (AionBlock) getBlockStore().getBlockByHash(bestBlock.getAntiparentHash());
        } else {
            throw new IllegalStateException("Invalid block type");
        }
    }

    public void loadBestStakingBlock() {
        long bestBlockNumber = bestBlock.getNumber();

        if (bestStakingBlock == null) {
            if (bestBlockNumber == 0) {
                bestStakingBlock = CfgAion.inst().getGenesisStakingBlock();
            } else {
                if (bestBlock.getHeader().getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {
                    bestStakingBlock = (StakingBlock) bestBlock;
                } else {
                    bestStakingBlock = (StakingBlock) getBlockStore().getBlockByHash(bestBlock.getAntiparentHash());

                    if (bestStakingBlock == null) {
                        bestStakingBlock = CfgAion.inst().getGenesisStakingBlock();
                    }
                }
            }
        }
    }

    /**
     * Imports a trie node to the indicated blockchain database.
     *
     * @param key the hash key of the trie node to be imported
     * @param value the value of the trie node to be imported
     * @param dbType the database where the key-value pair should be stored
     * @throws IllegalArgumentException if the given key is null
     * @return a {@link TrieNodeResult} indicating the success or failure of the import operation
     */
    public TrieNodeResult importTrieNode(byte[] key, byte[] value, DatabaseType dbType) {
        return repository.importTrieNode(key, value, dbType);
    }

    /**
     * If using TOP pruning we need to check the pruning restriction for the block. Otherwise, there
     * is not prune restriction.
     */
    public boolean hasPruneRestriction() {
        // no restriction when not in TOP pruning mode
        return repository.usesTopPruning();
    }

    /**
     * Heuristic for skipping the call to tryToConnect with block number that was already pruned.
     */
    public boolean isPruneRestricted(long blockNumber) {
        // no restriction when not in TOP pruning mode
        if (!hasPruneRestriction()) {
            return false;
        }
        return blockNumber < bestBlockNumber.get() - repository.getPruneBlockCount() + 1;
    }

    /**
     * Import block without validity checks and creating the state. Cannot be used for storing the
     * pivot which will not have a parent present in the database.
     *
     * @param block the block to be imported
     * @return a result describing the status of the attempted import
     */
    public synchronized FastImportResult tryFastImport(final Block block) {
        if (block == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fast sync import attempted with null block or header.");
            }
            return FastImportResult.INVALID_BLOCK;
        }
        if (block.getTimestamp()
                > (TimeUtils.currentTimeSecs()
                        + this.chainConfiguration.getConstants().getClockDriftBufferTime())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Block {} invalid due to timestamp {}.",
                        block.getShortHash(),
                        block.getTimestamp());
            }
            return FastImportResult.INVALID_BLOCK;
        }

        // check that the block is not already known
        Block known = getBlockStore().getBlockByHash(block.getHash());
        if (known != null && known.getNumber() == block.getNumber()) {
            return FastImportResult.KNOWN;
        }

        // a child must be present to import the parent
        Block child = getBlockStore().getChainBlockByNumber(block.getNumber() + 1);
        if (child == null || !Arrays.equals(child.getParentHash(), block.getHash())) {
            return FastImportResult.NO_CHILD;
        } else {
            // the total difficulty will be updated after the chain is complete
            getBlockStore().saveBlock(block, ZERO, ZERO,true);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Fast sync block saved: number: {}, hash: {}, child: {}",
                        block.getNumber(),
                        block.getShortHash(),
                        child.getShortHash());
            }
            return FastImportResult.IMPORTED;
        }
    }

    /**
     * Walks though the ancestor blocks starting with the given hash to determine if there is an
     * ancestor missing from storage. Returns the ancestor's hash if one is found missing or {@code
     * null} when the history is complete, i.e. no missing ancestors exist.
     *
     * @param block the first block to be checked if present in the repository
     * @return the ancestor's hash and height if one is found missing or {@code null} when the
     *     history is complete
     * @throws NullPointerException when given a null block as input
     */
    public Pair<ByteArrayWrapper, Long> findMissingAncestor(Block block) {
        Objects.requireNonNull(block);

        // initialize with given parameter
        byte[] currentHash = block.getHash();
        long currentNumber = block.getNumber();

        Block known = getBlockStore().getBlockByHash(currentHash);

        while (known != null && known.getNumber() > 0) {
            currentHash = known.getParentHash();
            currentNumber--;
            known = getBlockStore().getBlockByHash(currentHash);
        }

        if (known == null) {
            return Pair.of(ByteArrayWrapper.wrap(currentHash), currentNumber);
        } else {
            return null;
        }
    }

    public static long shutdownHook = Long.MAX_VALUE;

    public synchronized ImportResult tryToConnect(final Block block) {
        if (bestBlock.getNumber() == shutdownHook) {
            LOG.info("Shutting down and dumping heap as indicated by CLI request since block number {} was reached.", shutdownHook);

            try {
                HeapDumper.dumpHeap(new File(System.currentTimeMillis() + "-heap-report.hprof").getAbsolutePath(), true);
            } catch (Exception e) {
                LOG.error("Unable to dump heap due to exception:", e);
            }

            // requested shutdown
            System.exit(SystemExitCodes.NORMAL);
        }
        return tryToConnectInternal(block, TimeUtils.currentTimeSecs());
    }

    public synchronized void compactState() {
        repository.compactState();
    }

    // TEMPORARY: here to support the ConsensusTest
    public Pair<ImportResult, AionBlockSummary> tryToConnectAndFetchSummary(
            Block block, long currTimeSeconds, boolean doExistCheck) {
        // Check block exists before processing more rules
        if (doExistCheck // skipped when redoing imports
                && getBlockStore().getMaxNumber() >= block.getNumber()
                && getBlockStore().isBlockStored(block.getHash(), block.getNumber())) {

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Block already exists hash: {}, number: {}",
                        block.getShortHash(),
                        block.getNumber());
            }

            if (!repository.isValidRoot(block.getStateRoot())) {
                // correct the world state for this block
                recoverWorldState(repository, block);
            }

            if (!repository.isIndexed(block.getHash(), block.getNumber())) {
                // correct the index for this block
                recoverIndexEntry(repository, block);
            }

            // retry of well known block
            return Pair.of(EXIST, null);
        }

        if (block.getTimestamp()
                > (currTimeSeconds
                        + this.chainConfiguration.getConstants().getClockDriftBufferTime())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Block {} invalid due to timestamp {}.",
                        Hex.toHexString(block.getHash()),
                        block.getTimestamp());
            }
            return Pair.of(INVALID_BLOCK, null);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Try connect block hash: {}, number: {}",
                    block.getShortHash(),
                    block.getNumber());
        }

        final ImportResult ret;

        // The simple case got the block
        // to connect to the main chain
        final AionBlockSummary summary;
        if (bestBlock.isParentOf(block)) {
            repository.syncToRoot(bestBlock.getStateRoot());

            // because the bestBlock is a parent this is the first block of its height
            // unless there was a recent fork it's likely we will add a mainchain block
            if (forkLevel == NO_FORK_LEVEL) {
                executionTypeForAVM = BlockCachingContext.MAINCHAIN;
                cachedBlockNumberForAVM = bestBlock.getNumber();
            } else {
                executionTypeForAVM = BlockCachingContext.SWITCHING_MAINCHAIN;
                cachedBlockNumberForAVM = forkLevel;
            }

            summary = add(block);
            ret = summary == null ? INVALID_BLOCK : IMPORTED_BEST;

            if (executionTypeForAVM == BlockCachingContext.SWITCHING_MAINCHAIN
                    && ret == IMPORTED_BEST) {
                // overwrite recent fork info after this
                forkLevel = NO_FORK_LEVEL;
            }
        } else {
            if (getBlockStore().isBlockStored(block.getParentHash(), block.getNumber()-1)) {
                BigInteger oldTotalDiff = getInternalTD();

                // determine if the block parent is main chain or side chain
                long parentHeight = block.getNumber() - 1; // inferred parent number
                if (getBlockStore().isMainChain(block.getParentHash(), parentHeight)) {
                    // main chain parent, therefore can use its number for getting the cache
                    executionTypeForAVM = BlockCachingContext.SIDECHAIN;
                    cachedBlockNumberForAVM = parentHeight;
                } else {
                    // side chain parent, therefore do not know the closes main chain block
                    executionTypeForAVM = BlockCachingContext.DEEP_SIDECHAIN;
                    cachedBlockNumberForAVM = 0;
                }

                summary = tryConnectAndFork(block);
                ret =
                        summary == null
                                ? INVALID_BLOCK
                                : (isMoreThan(getInternalTD(), oldTotalDiff)
                                        ? IMPORTED_BEST
                                        : IMPORTED_NOT_BEST);
            } else {
                summary = null;
                ret = NO_PARENT;
            }
        }

        // update best block reference
        if (ret == IMPORTED_BEST) {
            pubBestBlock = bestBlock;
        }

        // fire block events
        if (ret.isSuccessful()) {
            if (this.evtMgr != null) {

                List<IEvent> evts = new ArrayList<>();
                IEvent evtOnBlock = new EventBlock(EventBlock.CALLBACK.ONBLOCK0);
                evtOnBlock.setFuncArgs(Collections.singletonList(summary));
                evts.add(evtOnBlock);

                IEvent evtTrace = new EventBlock(EventBlock.CALLBACK.ONTRACE0);
                String str = String.format("Block chain size: [ %d ]", this.getSizeInternal());
                evtTrace.setFuncArgs(Collections.singletonList(str));
                evts.add(evtTrace);

                if (ret == IMPORTED_BEST) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("IMPORTED_BEST");
                    }
                    IEvent evtOnBest = new EventBlock(EventBlock.CALLBACK.ONBEST0);
                    evtOnBest.setFuncArgs(Arrays.asList(block, summary.getReceipts()));
                    evts.add(evtOnBest);
                }

                this.evtMgr.newEvents(evts);
            }
        }

        if (ret == IMPORTED_BEST) {
            if (TX_LOG.isDebugEnabled()) {
                for (AionTxReceipt receipt : summary.getReceipts()) {
                    if (receipt != null) {
                        byte[] transactionHash = receipt.getTransaction().getTransactionHash();
                        TX_LOG.debug(
                                "Transaction: "
                                        + Hex.toHexString(transactionHash)
                                        + " was sealed into block #"
                                        + block.getNumber());
                    }
                }
            }
        }

        return Pair.of(ret, summary);
    }

    /**
     * Try to import the block without flush the repository
     *
     * @param block the block trying to import
     * @return import result and summary
     */
    public Pair<AionBlockSummary, RepositoryCache> tryImportWithoutFlush(final Block block) {
        repository.syncToRoot(bestBlock.getStateRoot());
        return add(block, false, false);
    }

    /**
     * Processes a new block and potentially appends it to the blockchain, thereby changing the
     * state of the world. Decoupled from wrapper function {@link #tryToConnect(Block)} so we
     * can feed timestamps manually
     */
    ImportResult tryToConnectInternal(final Block block, long currTimeSeconds) {
        return tryToConnectAndFetchSummary(block, currTimeSeconds, true).getLeft();
    }

    /**
     * Creates a new mining block, if you require more context refer to the blockContext creation method,
     * which allows us to add metadata not usually associated with the block itself.
     *
     * @param parent block
     * @param transactions to be added into the block
     * @param waitUntilBlockTime if we should wait until the specified blockTime before create a new
     *     block
     * @see #createNewMiningBlock(Block, List, boolean)
     * @return new block
     */
    public synchronized AionBlock createNewMiningBlock(Block parent, List<AionTransaction> transactions, boolean waitUntilBlockTime) {
        return createNewMiningBlockContext(parent, transactions, waitUntilBlockTime).block;
    }

    /**
     * A method for creating a new staking block template for the internal staker.
     *
     * @param parent the parent block of the chain, it is not equal to the seal parent block.
     * @param transactions to be added into the block.
     * @param seed the data decide the weight of the sealing difficulty.
     * @see #createNewStakingBlock(Block, List, byte[], byte[])
     * @return staking block template
     */
    public StakingBlock createNewStakingBlock(Block parent, List<AionTransaction> transactions, byte[] seed) {
        if (parent == null || transactions == null || seed == null) {
            throw new NullPointerException();
        }

        return createNewStakingBlock(parent, transactions, seed, null);
    }

    private synchronized StakingBlock createNewStakingBlock(Block parent, List<AionTransaction> txs, byte[] newSeed, byte[] publicKey) {
        if (parent == null || txs == null || newSeed == null) {
            throw new NullPointerException();
        }

        BlockHeader parentHdr = parent.getHeader();

        if (parentHdr.getNumber() + 1 < FORK_5_BLOCK_NUMBER) {
            LOG.debug("Unity fork has not been enabled! Can't create the staking blocks");
            return null;
        }

        Block grandParentStakingBlock = null;
        BlockHeader parentStakingBlockHeader = null;

        if (parentHdr.getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {
            parentStakingBlockHeader = parentHdr;
            grandParentStakingBlock = getParentBlock(parentHdr);
        } else if (parentHdr.getSealType() == BlockSealType.SEAL_POW_BLOCK.getSealId()) {

            if (Arrays.equals(
                    parent.getAntiparentHash(),
                    CfgAion.inst().getGenesisStakingBlock().getHash())) {
                parentStakingBlockHeader = CfgAion.inst().getGenesisStakingBlock().getHeader();
                grandParentStakingBlock = null;
            } else {
                Block parentMiningBlock = getBlockByHash(parent.getAntiparentHash());
                if (parentMiningBlock != null) {
                    parentStakingBlockHeader = parentMiningBlock.getHeader();
                    grandParentStakingBlock = getParentBlock(parentStakingBlockHeader);
                }
            }
        } else {
            throw new IllegalStateException("Invalid block type");
        }

        if (parentStakingBlockHeader == null) {
            throw new IllegalStateException(
                    "Can't find the parent staking block, the Database might be corrupted!");
        }

        AionAddress coinbase = stakerCoinbase;
        long newTimestamp;

        BigInteger newDiff =
                chainConfiguration
                        .getUnityDifficultyCalculator()
                        .calculateDifficulty(
                                parentStakingBlockHeader,
                                grandParentStakingBlock == null
                                        ? null
                                        : grandParentStakingBlock.getHeader());

        if (publicKey != null) { // Create block template for the external stakers.
            AionAddress signingAddress = new AionAddress(AddressSpecs.computeA0Address(publicKey));
            coinbase = stakingContractHelper.getCoinbaseForSigningAddress(signingAddress);
            if (coinbase == null) {
                LOG.debug("Could not get the coinbase by given the signing publickey", ByteUtil.toHexString(publicKey));
                return null;
            }

            byte[] seed = ((StakedBlockHeader) parentStakingBlockHeader).getSeed();
            if (!ECKeyEd25519.verify(seed, newSeed, publicKey)) {
                LOG.debug(
                        "Seed verification failed! oldSeed:{} newSeed{} pKey{}",
                        ByteUtil.toHexString(seed),
                        ByteUtil.toHexString(newSeed),
                        ByteUtil.toHexString(publicKey));
                return null;
            }

            BigInteger stakes = stakingContractHelper.getEffectiveStake(signingAddress, coinbase);
            if (stakes.signum() < 1) {
                LOG.debug("The caller {} with coinbase {} has no stake ", signingAddress.toString(), coinbase.toString());
                return null;
            }

            long newDelta =
                max(
                    (long) (newDiff.doubleValue()
                        * (Math.log(BigInteger.TWO.pow(256).doubleValue())
                            - Math.log(new BigInteger(1, HashUtil.h256(newSeed)).doubleValue()))
                        / stakes.longValue()),
                    1);

            newTimestamp =
                max(
                    parentStakingBlockHeader.getTimestamp() + newDelta,
                    parent.getHeader().getTimestamp() + 1);
        } else {
            newTimestamp = TimeUtils.currentTimeSecs();

            if (parentHdr.getTimestamp() >= newTimestamp) {
                newTimestamp = parentHdr.getTimestamp() + 1;
            }
        }

        if (coinbase == null) {
            throw new NullPointerException(
                    "Invalid coinbase address, please check your stakerCoinbase settings or the coinbase of the external staker ");
        }

        StakingBlock block;
        try {
            StakedBlockHeader.Builder headerBuilder =
                    new StakedBlockHeader.Builder()
                            .withParentHash(parent.getHash())
                            .withCoinbase(coinbase)
                            .withNumber(parentHdr.getNumber() + 1)
                            .withTimestamp(newTimestamp)
                            .withExtraData(minerExtraData)
                            .withTxTrieRoot(calcTxTrie(txs))
                            .withEnergyLimit(energyLimitStrategy.getEnergyLimit(parentHdr))
                            .withDifficulty(
                                    ByteUtil.bigIntegerToBytes(newDiff, DIFFICULTY_BYTES))
                            .withSeed(newSeed);
            if (publicKey != null) {
                headerBuilder.withPubKey(publicKey);
            }
            block = new StakingBlock(headerBuilder.build(), txs);
        } catch (HeaderStructureException e) {
            throw new RuntimeException(e);
        }

        blockPreSeal(parentHdr, block);

        if (publicKey != null) {
            stakingBlockTemplate.put(
                    ByteArrayWrapper.wrap(block.getHeader().getMineHash()), block);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("GetBlockTemp: {}", block.toString());
        }

        return block;
    }

    private synchronized BigInteger blockPreSeal(BlockHeader parentHdr, Block block) {
        /*
         * Begin execution phase
         */
        pushState(parentHdr.getHash());

        track = repository.startTracking();

        RetValidPreBlock preBlock = generatePreBlock(block);

        track.flush();

        /*
         * Calculate the gas used for the included transactions
         */
        long totalEnergyUsed = 0;
        BigInteger totalTransactionFee = BigInteger.ZERO;
        for (AionTxExecSummary summary : preBlock.summaries) {
            totalEnergyUsed = totalEnergyUsed + summary.getNrgUsed().longValueExact();
            totalTransactionFee = totalTransactionFee.add(summary.getFee());
        }

        byte[] stateRoot = getRepository().getRoot();
        popState();

        /*
         * End execution phase
         */
        Bloom logBloom = new Bloom();
        for (AionTxReceipt receipt : preBlock.receipts) {
            logBloom.or(receipt.getBloomFilter());
        }

        if (block instanceof AionBlock) {
            ((AionBlock)block).seal(
                preBlock.txs,
                calcTxTrie(preBlock.txs),
                stateRoot,
                logBloom.getBloomFilterBytes(),
                calcReceiptsTrie(preBlock.receipts),
                totalEnergyUsed);
        } else if (block instanceof StakingBlock) {
            ((StakingBlock)block).seal(
                preBlock.txs,
                calcTxTrie(preBlock.txs),
                stateRoot,
                logBloom.getBloomFilterBytes(),
                calcReceiptsTrie(preBlock.receipts),
                totalEnergyUsed);
        } else {
            throw new IllegalStateException("Invalid block class!" + block.getClass().getName());
        }

        return totalTransactionFee;
    }

    /**
     * Creates a new mining block, adding in context/metadata about the block
     *
     * @param parent the parent block
     * @param txs to be added into the block
     * @param waitUntilBlockTime if we should wait until the specified blockTime before create a new
     *     block
     * @see #createNewMiningBlockContext(Block, List, boolean)
     * @return a context with new mining block
     */
    public synchronized BlockContext createNewMiningBlockContext(
            Block parent, List<AionTransaction> txs, boolean waitUntilBlockTime) {
            return createNewMiningBlockInternal(
                parent, txs, waitUntilBlockTime, TimeUtils.currentTimeSecs());
    }

    BlockContext createNewMiningBlockInternal(
            Block parent,
            List<AionTransaction> txs,
            boolean waitUntilBlockTime,
            long currTimeSeconds) {

        BlockHeader parentHdr = parent.getHeader();

        long time = currTimeSeconds;
        if (parentHdr.getTimestamp() >= time) {
            time = parentHdr.getTimestamp() + 1;
            while (waitUntilBlockTime && TimeUtils.currentTimeSecs() <= time) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        long energyLimit = this.energyLimitStrategy.getEnergyLimit(parentHdr);

        AionBlock block;
        try {
            A0BlockHeader.Builder headerBuilder =
                    new A0BlockHeader.Builder()
                            .withParentHash(parent.getHash())
                            .withCoinbase(minerCoinbase)
                            .withNumber(parentHdr.getNumber() + 1)
                            .withTimestamp(time)
                            .withExtraData(minerExtraData)
                            .withTxTrieRoot(calcTxTrie(txs))
                            .withEnergyLimit(energyLimit);
            block = new AionBlock(headerBuilder.build(), txs);
        } catch (HeaderStructureException e) {
            throw new RuntimeException(e);
        }

        Block grandParentMiningBlock = null;
        BlockHeader parentMiningBlockHeader = null;

        if (parentHdr.getSealType() == BlockSealType.SEAL_POW_BLOCK.getSealId()) {
            parentMiningBlockHeader = parentHdr;
            grandParentMiningBlock = getParentBlock(parentHdr);
        } else if (parentHdr.getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {
            Block parentMiningBlock = getBlockByHash(parent.getAntiparentHash());
            if (parentMiningBlock != null) {
                parentMiningBlockHeader = parentMiningBlock.getHeader();
                grandParentMiningBlock = getParentBlock(parentMiningBlock.getHeader());
            }
        } else {
            throw new IllegalStateException("Invalid block type");
        }

        if (block.getNumber() >= FORK_5_BLOCK_NUMBER) {
            block.getHeader()
                    .setDifficulty(
                            ByteUtil.bigIntegerToBytes(
                                    chainConfiguration
                                            .getUnityDifficultyCalculator()
                                            .calculateDifficulty(
                                                    parentMiningBlockHeader,
                                                    grandParentMiningBlock == null
                                                            ? null
                                                            : grandParentMiningBlock
                                                                    .getHeader()),
                                    DIFFICULTY_BYTES));
        } else {
            block.getHeader()
                    .setDifficulty(
                            ByteUtil.bigIntegerToBytes(
                                    this.chainConfiguration
                                            .getDifficultyCalculator()
                                            .calculateDifficulty(
                                                    parentMiningBlockHeader,
                                                    grandParentMiningBlock == null
                                                            ? null
                                                            : grandParentMiningBlock
                                                                    .getHeader()),
                                    DIFFICULTY_BYTES));
        }

        BigInteger totalTransactionFee = blockPreSeal(parentHdr, block);

        // derive base block reward
        BigInteger baseBlockReward =
                this.chainConfiguration
                        .getRewardsCalculator()
                        .calculateReward(block.getHeader().getNumber());
        return new BlockContext(block, baseBlockReward, totalTransactionFee);
    }

    private AionBlockSummary add(Block block) {
        // typical use without rebuild
        AionBlockSummary summary = add(block, false);

        if (summary != null) {
            updateTotalDifficulty(block);
            summary.setTotalDifficulty(block.getCumulativeDifficulty());

            storeBlock(block, summary.getReceipts(), summary.getSummaries());

            flush();
        }

        return summary;
    }

    private AionBlockSummary add(Block block, boolean rebuild) {
        return add(block, rebuild, true).getLeft();
    }

    /** @Param flushRepo true for the kernel runtime import and false for the DBUtil */
    public Pair<AionBlockSummary, RepositoryCache> add(
            Block block, boolean rebuild, boolean flushRepo) {
        // reset cached VMs before processing the block
        repository.clearCachedVMs();

        if (!isValid(block)) {
            LOG.error("Attempting to add {} block.", (block == null ? "NULL" : "INVALID"));
            return Pair.of(null, null);
        }

        track = repository.startTracking();
        byte[] origRoot = repository.getRoot();

        // (if not reconstructing old blocks) keep chain continuity
        if (!rebuild && !Arrays.equals(bestBlock.getHash(), block.getParentHash())) {
            LOG.error("Attempting to add NON-SEQUENTIAL block.");
            return Pair.of(null, null);
        }

        if (rebuild) {
            // when recovering blocks do not touch the cache
            executionTypeForAVM = BlockCachingContext.DEEP_SIDECHAIN;
            cachedBlockNumberForAVM = 0;
        }

        AionBlockSummary summary = processBlock(block);
        List<AionTxReceipt> receipts = summary.getReceipts();

        // Sanity checks
        byte[] receiptHash = block.getReceiptsRoot();
        byte[] receiptListHash = calcReceiptsTrie(receipts);

        if (!Arrays.equals(receiptHash, receiptListHash)) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        "Block's given Receipt Hash doesn't match: {} != {}",
                        receiptHash,
                        receiptListHash);
                LOG.warn("Calculated receipts: " + receipts);
            }
            track.rollback();
            return Pair.of(null, null);
        }

        byte[] logBloomHash = block.getLogBloom();
        byte[] logBloomListHash = calcLogBloom(receipts);

        if (!Arrays.equals(logBloomHash, logBloomListHash)) {
            if (LOG.isWarnEnabled())
                LOG.warn(
                        "Block's given logBloom Hash doesn't match: {} != {}",
                        ByteUtil.toHexString(logBloomHash),
                        ByteUtil.toHexString(logBloomListHash));
            track.rollback();
            return Pair.of(null, null);
        }

        if (!flushRepo) {
            return Pair.of(summary, track);
        }

        track.flush();
        repository.commitCachedVMs(block.getHashWrapper());

        if (!rebuild) {
            byte[] blockStateRootHash = block.getStateRoot();
            byte[] worldStateRootHash = repository.getRoot();

            if (!Arrays.equals(blockStateRootHash, worldStateRootHash)) {

                LOG.warn(
                        "BLOCK: State conflict or received invalid block. block: {} worldstate {} mismatch",
                        block.getNumber(),
                        worldStateRootHash);
                LOG.warn("Conflict block dump: {}", toHexString(block.getEncoded()));

                // block is bad so 'rollback' the state root to the original state
                repository.setRoot(origRoot);
                return Pair.of(null, null);
            }
        }

        if (rebuild) {
            List<AionTxExecSummary> execSummaries = summary.getSummaries();
            AionTxInfo info;
            for (int i = 0; i < receipts.size(); i++) {
                if (storeInternalTransactions) {
                    info = AionTxInfo.newInstanceWithInternalTransactions(receipts.get(i), block.getHash(), i, execSummaries.get(i).getInternalTransactions());
                } else {
                    info = AionTxInfo.newInstance(receipts.get(i), block.getHash(), i);
                }
                transactionStore.putToBatch(info);
            }
            transactionStore.flushBatch();

            repository.commitBlock(block.getHashWrapper(), block.getNumber(), block.getStateRoot());

            if (LOG.isDebugEnabled())
                LOG.debug(
                        "Block rebuilt: number: {}, hash: {}, TD: {}",
                        block.getNumber(),
                        block.getShortHash(),
                        getTotalDifficulty());
        }

        return Pair.of(summary, null);
    }

    public void flush() {
        repository.flush();
        try {
            getBlockStore().flush();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        transactionStore.flush();
    }

    @SuppressWarnings("unused")
    private boolean needFlushByMemory(double maxMemoryPercents) {
        return getRuntime().freeMemory() < (getRuntime().totalMemory() * (1 - maxMemoryPercents));
    }

    private Block getParent(BlockHeader header) {
        return getBlockStore().getBlockByHashWithInfo(header.getParentHash());
    }

    private Block getParentBlock(BlockHeader header) {
        if (header.isGenesis()) {
            return null;
        }

        Block parent = getParent(header);
        if (parent.getHeader().getSealType() == header.getSealType()) {
            return parent;
        } else {
            if (Arrays.equals(parent.getAntiparentHash(), CfgAion.inst().getGenesisStakingBlock().getHash())) {
                return CfgAion.inst().getGenesisStakingBlock();
            }

            Block antiParentBlock = getBlockStore().getBlockByHashWithInfo(parent.getAntiparentHash());
            if (antiParentBlock == null) {
                return null;
            }

            return antiParentBlock;
        }
    }

    public boolean isValid(BlockHeader header) {
        if (header.getSealType() == BlockSealType.SEAL_POW_BLOCK.getSealId()) {
            /*
             * Header should already be validated at this point, no need to check again
             * 1. Block came in from network; validated by P2P before processing further
             * 2. Block was submitted locally - adding invalid data to your own chain
             */
            //        if (!this.blockHeaderValidator.validate(header, LOG)) {
            //            return false;
            //        }

            Block parent = getParent(header);
            if (parent == null) {
                return false;
            }

            Block sealParent;
            if (parent.getHeader().getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {
                sealParent = getBlockByHash(parent.getAntiparentHash());
            } else {
                sealParent = parent;
            }

            if (sealParent == null) {
                throw new IllegalStateException("Can't find the sealParent block, the database might corrupt!");
            }

            if (!chainParentBlockHeaderValidator.validate(header, parent.getHeader(), LOG, null)) {
                return false;
            }

            Block grandSealParent = getParentBlock(sealParent.getHeader());

            if (header.getNumber() >= FORK_5_BLOCK_NUMBER) {
                return unityGrandParentBlockHeaderValidator.validate(
                    grandSealParent == null ? null : grandSealParent.getHeader(),
                    sealParent.getHeader(),
                    header,
                    LOG);
            } else {
                return preUnityGrandParentBlockHeaderValidator.validate(
                    grandSealParent == null ? null : grandSealParent.getHeader(),
                    sealParent.getHeader(),
                    header,
                    LOG);
            }
        } else if (header.getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {

            if (header.getNumber() < FORK_5_BLOCK_NUMBER) {
                return false;
            }

            Block parent = getParent(header);
            if (parent == null) {
                return false;
            }

            if (!chainParentBlockHeaderValidator.validate(header, parent.getHeader(), LOG, null)) {
                return false;
            }

            Block sealParent;
            if (parent.getHeader().getSealType() == BlockSealType.SEAL_POW_BLOCK.getSealId()) {
                sealParent = getBlockByHash(parent.getAntiparentHash());
                if (sealParent == null) {
                    sealParent = CfgAion.inst().getGenesisStakingBlock();
                }
            } else {
                sealParent = parent;
            }

            BigInteger stake =
                getStakingContractHelper()
                    .getEffectiveStake(
                        new AionAddress(AddressSpecs.computeA0Address(header.getSigningPublicKey())),
                        header.getCoinbase());

            if (!sealParentBlockHeaderValidator.validate(header, sealParent.getHeader(), LOG, stake)) {
                return false;
            }

            Block grandSealParent = parent.isGenesis() ? null : getParentBlock(sealParent.getHeader());
            return unityGrandParentBlockHeaderValidator.validate(
                    grandSealParent == null ? null : grandSealParent.getHeader(),
                    sealParent.getHeader(),
                    header,
                    LOG);

        } else {
            LOG.debug("Invalid header seal type!");
            return false;
        }
    }

    /**
     * This mechanism enforces a homeostasis in terms of the time between blocks; a smaller period
     * between the last two blocks results in an increase in the difficulty level and thus
     * additional computation required, lengthening the likely next period. Conversely, if the
     * period is too large, the difficulty, and expected time to the next block, is reduced.
     */
    private boolean isValid(Block block) {

        if (block == null) {
            return false;
        }

        if (!block.isGenesis()) {
            if (!isValid(block.getHeader())) {
                return false;
            }

            // Sanity checks
            byte[] trieHash = block.getTxTrieRoot();
            List<AionTransaction> txs = block.getTransactionsList();

            byte[] trieListHash = calcTxTrie(txs);
            if (!Arrays.equals(trieHash, trieListHash)) {
                LOG.warn(
                        "Block's given Trie Hash doesn't match: {} != {}",
                        toHexString(trieHash),
                        toHexString(trieListHash));
                return false;
            }

            if (txs != null && !txs.isEmpty()) {
                Repository parentRepo = repository;

                if (!Arrays.equals(bestBlock.getHash(), block.getParentHash())) {
                    parentRepo =
                            repository.getSnapshotTo(
                                    getBlockByHash(block.getParentHash()).getStateRoot());
                }

                Map<AionAddress, BigInteger> nonceCache = new HashMap<>();

                if (txs.parallelStream()
                        .anyMatch(
                                tx ->
                                        !TXValidator.isValid(tx)
                                                || !TransactionTypeValidator.isValid(tx))) {
                    LOG.error("Some transactions in the block are invalid");
                    if (TX_LOG.isDebugEnabled()) {
                        for (AionTransaction tx : txs) {
                            TX_LOG.debug(
                                    "Tx valid ["
                                            + TXValidator.isValid(tx)
                                            + "]. Type valid ["
                                            + TransactionTypeValidator.isValid(tx)
                                            + "]\n"
                                            + tx.toString());
                        }
                    }
                    return false;
                }

                for (AionTransaction tx : txs) {
                    AionAddress txSender = tx.getSenderAddress();

                    BigInteger expectedNonce = nonceCache.get(txSender);

                    if (expectedNonce == null) {
                        expectedNonce = parentRepo.getNonce(txSender);
                    }

                    BigInteger txNonce = tx.getNonceBI();
                    if (!expectedNonce.equals(txNonce)) {
                        LOG.warn(
                                "Invalid transaction: Tx nonce {} != expected nonce {} (parent nonce: {}): {}",
                                txNonce.toString(),
                                expectedNonce.toString(),
                                parentRepo.getNonce(txSender),
                                tx);
                        return false;
                    }

                    // update cache
                    nonceCache.put(txSender, expectedNonce.add(BigInteger.ONE));
                }
            }
        }

        return true;
    }

    private AionBlockSummary processBlock(Block block) {

        if (!block.isGenesis()) {
            return applyBlock(block);
        } else {
            return new AionBlockSummary(
                    block, new HashMap<>(), new ArrayList<>(), new ArrayList<>());
        }
    }

    /**
     * For generating the necessary transactions for a block
     *
     * @param block
     * @return
     */
    private RetValidPreBlock generatePreBlock(Block block) {

        long saveTime = System.nanoTime();

        List<AionTxReceipt> receipts = new ArrayList<>();
        List<AionTxExecSummary> summaries = new ArrayList<>();
        List<AionTransaction> transactions = new ArrayList<>();

        if (!block.getTransactionsList().isEmpty()) {

            fork040Enable = checkFork040(block.getNumber());
            if (fork040Enable) {
                TransactionTypeRule.allowAVMContractTransaction();
            }

            try {
                // Booleans moved out here so their meaning is explicit.
                boolean isLocalCall = false;
                boolean incrementSenderNonce = true;
                boolean checkBlockEnergyLimit = true;

                List<AionTxExecSummary> executionSummaries =
                        BulkExecutor.executeAllTransactionsInBlock(
                                block.getDifficulty(),
                                block.getNumber(),
                                block.getTimestamp(),
                                block.getNrgLimit(),
                                block.getCoinbase(),
                                block.getTransactionsList(),
                                track,
                                isLocalCall,
                                incrementSenderNonce,
                                fork040Enable,
                                checkBlockEnergyLimit,
                                LOGGER_VM,
                                getPostExecutionWorkForGeneratePreBlock(repository),
                                BlockCachingContext.PENDING,
                                bestBlock.getNumber());

                for (AionTxExecSummary summary : executionSummaries) {
                    if (!summary.isRejected()) {
                        transactions.add(summary.getTransaction());
                        receipts.add(summary.getReceipt());
                        summaries.add(summary);
                    }
                }
            } catch (VMException e) {
                LOG.error("Shutdown due to a VM fatal error.", e);
                System.exit(SystemExitCodes.FATAL_VM_ERROR);
            }
        }

        Map<AionAddress, BigInteger> rewards = addReward(block);

        long totalTime = System.nanoTime() - saveTime;
        chainStats.addBlockExecTime(totalTime);
        return new RetValidPreBlock(transactions, rewards, receipts, summaries);
    }

    private AionBlockSummary applyBlock(Block block) {
        long saveTime = System.nanoTime();

        List<AionTxReceipt> receipts = new ArrayList<>();
        List<AionTxExecSummary> summaries = new ArrayList<>();

        if (!block.getTransactionsList().isEmpty()) {

            // might apply the block before the 040 fork point.
            fork040Enable = checkFork040(block.getNumber());
            if (fork040Enable) {
                TransactionTypeRule.allowAVMContractTransaction();
            }

            try {
                // Booleans moved out here so their meaning is explicit.
                boolean isLocalCall = false;
                boolean incrementSenderNonce = true;
                boolean checkBlockEnergyLimit = false;

                List<AionTxExecSummary> executionSummaries =
                        BulkExecutor.executeAllTransactionsInBlock(
                                block.getDifficulty(),
                                block.getNumber(),
                                block.getTimestamp(),
                                block.getNrgLimit(),
                                block.getCoinbase(),
                                block.getTransactionsList(),
                                track,
                                isLocalCall,
                                incrementSenderNonce,
                                fork040Enable,
                                checkBlockEnergyLimit,
                                LOGGER_VM,
                                getPostExecutionWorkForApplyBlock(repository),
                                executionTypeForAVM,
                                cachedBlockNumberForAVM);

                for (AionTxExecSummary summary : executionSummaries) {
                    receipts.add(summary.getReceipt());
                    summaries.add(summary);
                }
            } catch (VMException e) {
                LOG.error("Shutdown due to a VM fatal error.", e);
                System.exit(SystemExitCodes.FATAL_VM_ERROR);
            }
        }
        Map<AionAddress, BigInteger> rewards = addReward(block);

        long totalTime = System.nanoTime() - saveTime;
        chainStats.addBlockExecTime(totalTime);

        return new AionBlockSummary(block, rewards, receipts, summaries);
    }

    /**
     * Add reward to block- and every uncle coinbase assuming the entire block is valid.
     *
     * @param block object containing the header and uncles
     */
    private Map<AionAddress, BigInteger> addReward(Block block) {

        Map<AionAddress, BigInteger> rewards = new HashMap<>();
        BigInteger minerReward =
                this.chainConfiguration
                        .getRewardsCalculator()
                        .calculateReward(block.getHeader().getNumber());
        rewards.put(block.getCoinbase(), minerReward);

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "rewarding: {}np to {} for mining block {}",
                    minerReward,
                    block.getCoinbase(),
                    block.getNumber());
        }

        /*
         * Remaining fees (the ones paid to miners for running transactions) are
         * already paid for at a earlier point in execution.
         */
        track.addBalance(block.getCoinbase(), minerReward);
        return rewards;
    }

    public ChainConfiguration getChainConfiguration() {
        return chainConfiguration;
    }

    private void storeBlock(Block block, List<AionTxReceipt> receipts, List<AionTxExecSummary> summaries) {

        UnityDifficulty ud = unityDifficulty.get();

        if (fork) {
            getBlockStore()
                    .saveBlock(block, ud.totalMiningDifficulty, ud.totalStakingDifficulty, false);
        } else {
            getBlockStore()
                    .saveBlock(block, ud.totalMiningDifficulty, ud.totalStakingDifficulty, true);
        }

        AionTxInfo info;
        for (int i = 0; i < receipts.size(); i++) {
            if (storeInternalTransactions) {
                info = AionTxInfo.newInstanceWithInternalTransactions(receipts.get(i), block.getHash(), i, summaries.get(i).getInternalTransactions());
            } else {
                info = AionTxInfo.newInstance(receipts.get(i), block.getHash(), i);
            }
            transactionStore.putToBatch(info);
        }
        transactionStore.flushBatch();

        repository.commitBlock(block.getHashWrapper(), block.getNumber(), block.getStateRoot());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Block saved: number: {}, hash: {}, TD: {}",
                    block.getNumber(),
                    block.getShortHash(),
                    ud.totalDifficulty);

            LOG.debug("block added to the blockChain: index: [{}]", block.getNumber());
        }

        setBestBlock(block);
    }

    public boolean storePendingStatusBlock(Block block) {
        try {
            return repository.getPendingBlockStore().addStatusBlock(block);
        } catch (Exception e) {
            LOG.error("Unable to store status block in " + repository.toString() + " due to: ", e);
            return false;
        }
    }

    public int storePendingBlockRange(List<Block> blocks) {
        try {
            return repository.getPendingBlockStore().addBlockRange(blocks);
        } catch (Exception e) {
            LOG.error(
                    "Unable to store range of blocks in " + repository.toString() + " due to: ", e);
            return 0;
        }
    }

    public Map<ByteArrayWrapper, List<Block>> loadPendingBlocksAtLevel(long level) {
        try {
            return repository.getPendingBlockStore().loadBlockRange(level);
        } catch (Exception e) {
            LOG.error(
                    "Unable to retrieve stored blocks from " + repository.toString() + " due to: ",
                    e);
            return Collections.emptyMap();
        }
    }

    public long nextBase(long current, long knownStatus) {
        try {
            return repository.getPendingBlockStore().nextBase(current, knownStatus);
        } catch (Exception e) {
            LOG.error("Unable to generate next LIGHTNING request base due to: ", e);
            return current;
        }
    }

    public void dropImported(
            long level,
            List<ByteArrayWrapper> ranges,
            Map<ByteArrayWrapper, List<Block>> blocks) {
        try {
            repository.getPendingBlockStore().dropPendingQueues(level, ranges, blocks);
        } catch (Exception e) {
            LOG.error(
                    "Unable to delete used blocks from " + repository.toString() + " due to: ", e);
        }
    }

    public TransactionStore getTransactionStore() {
        return transactionStore;
    }

    public Block getBestBlock() {
        return pubBestBlock == null ? bestBlock : pubBestBlock;
    }

    public synchronized void setBestBlock(Block block) {
        bestBlock = block;
        if (bestBlock instanceof AionBlock) {
            bestMiningBlock = (AionBlock) bestBlock;
        } else if (bestBlock instanceof StakingBlock) {
            bestStakingBlock = (StakingBlock) bestBlock;
        } else {
            throw new IllegalStateException("Invalid Block instance");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("BestBlock {}", bestBlock.toString());
            if (bestMiningBlock != null) {
                LOG.debug("BestMiningBlock {}", bestMiningBlock.toString());
            }

            if (bestStakingBlock != null) {
                LOG.debug("BestStakingBlock {}", bestStakingBlock.toString());
            }
        }

        updateBestKnownBlock(block);
        bestBlockNumber.set(bestBlock.getNumber());
    }

    public synchronized void close() {
        getBlockStore().close();
    }

    public BigInteger getTotalDifficulty() {
        return getBestBlock().getCumulativeDifficulty();
    }

    public void setUnityTotalDifficulty(BigInteger totalDifficulty, BigInteger miningDifficulty,
        BigInteger stakingDifficulty) {
        if (totalDifficulty == null || miningDifficulty == null || stakingDifficulty == null) {
            throw new NullPointerException();
        }

        unityDifficulty.set(
                new UnityDifficulty(totalDifficulty, miningDifficulty, stakingDifficulty));
    }

    @VisibleForTesting
    BigInteger getCacheTD() {
        UnityDifficulty ud = unityDifficulty.get();
        return ud.totalMiningDifficulty.multiply(ud.totalStakingDifficulty);
    }

    private BigInteger getInternalTD() {
        return unityDifficulty.get().totalDifficulty;
    }

    private void updateTotalDifficulty(Block block) {

        UnityDifficulty ud = unityDifficulty.get();

        if(block.getNumber() == FORK_5_BLOCK_NUMBER) {
            unityDifficulty.set(
                    new UnityDifficulty(
                            ud.totalDifficulty,
                            ud.totalMiningDifficulty,
                            GenesisStakingBlock.getGenesisDifficulty()));
            ud = unityDifficulty.get();
        }

        BigInteger tmd = ud.totalMiningDifficulty;
        BigInteger tsd = ud.totalStakingDifficulty;
        if (block.getHeader().getSealType() == BlockSealType.SEAL_POW_BLOCK.getSealId()) {
            tmd = tmd.add(block.getDifficultyBI());
        } else if (block.getHeader().getSealType() == BlockSealType.SEAL_POS_BLOCK.getSealId()) {
            tsd = tsd.add(block.getDifficultyBI());
        } else {
            throw new IllegalStateException("Invalid block type");
        }

        unityDifficulty.set(new UnityDifficulty(tmd.multiply(tsd), tmd, tsd));

        block.setCumulativeDifficulty(unityDifficulty.get().totalDifficulty);
        if (LOG.isDebugEnabled()) {
            LOG.debug("TD: updated to {}", unityDifficulty.get().totalDifficulty);
        }
    }

    public void setExitOn(long exitOn) {
        this.exitOn = exitOn;
    }

    public AionAddress getMinerCoinbase() {
        return minerCoinbase;
    }

    @Override
    public boolean isBlockStored(byte[] hash, long number) {
        return getBlockStore().isBlockStored(hash, number);
    }

    /**
     * Returns up to limit headers found with following search parameters
     *
     * @param blockNumber Identifier of start block, by number
     * @param limit Maximum number of headers in return
     * @return {@link A0BlockHeader}'s list or empty list if none found
     */
    public List<BlockHeader> getListOfHeadersStartFrom(long blockNumber, int limit) {

        // identifying block we'll move from
        Block startBlock = getBlockByNumber(blockNumber);

        // if nothing found on main chain, return empty array
        if (startBlock == null) {
            return emptyList();
        }

        List<BlockHeader> headers;
        long bestNumber = bestBlock.getNumber();
        headers = getContinuousHeaders(bestNumber, blockNumber, limit);

        return headers;
    }

    /**
     * Finds up to limit blocks starting from blockNumber on main chain
     *
     * @param bestNumber Number of best block
     * @param blockNumber Number of block to start search (included in return)
     * @param limit Maximum number of headers in response
     * @return headers found by query or empty list if none
     */
    private List<BlockHeader> getContinuousHeaders(long bestNumber, long blockNumber, int limit) {
        int qty = getQty(blockNumber, bestNumber, limit);

        byte[] startHash = getStartHash(blockNumber, qty);

        if (startHash == null) {
            return emptyList();
        }

        List<BlockHeader> headers = getBlockStore().getListHeadersEndWith(startHash, qty);

        // blocks come with decreasing numbers
        Collections.reverse(headers);

        return headers;
    }

    private int getQty(long blockNumber, long bestNumber, int limit) {
        if (blockNumber + limit - 1 > bestNumber) {
            return (int) (bestNumber - blockNumber + 1);
        } else {
            return limit;
        }
    }

    private byte[] getStartHash(long blockNumber, int qty) {

        long startNumber;

        startNumber = blockNumber + qty - 1;

        Block block = getBlockByNumber(startNumber);

        if (block == null) {
            return null;
        }

        return block.getHash();
    }

    // NOTE: Functionality removed because not used and untested
    //    /**
    //     * Returns up to limit headers found with following search parameters
    //     *
    //     * @param identifier
    //     *            Identifier of start block, by number of by hash
    //     * @param skip
    //     *            Number of blocks to skip between consecutive headers
    //     * @param limit
    //     *            Maximum number of headers in return
    //     * @param reverse
    //     *            Is search reverse or not
    //     * @return {@link A0BlockHeader}'s list or empty list if none found
    //     */
    //    @Override
    //    public List<A0BlockHeader> getListOfHeadersStartFrom(BlockIdentifierImpl identifier, int
    // skip,
    // int limit,
    //            boolean reverse) {
    //
    //        // null identifier check
    //        if (identifier == null){
    //            return emptyList();
    //        }
    //
    //        // Identifying block we'll move from
    //        IAionBlock startBlock;
    //        if (identifier.getHash() != null) {
    //            startBlock = getBlockByHash(identifier.getHash());
    //        } else {
    //            startBlock = getBlockByNumber(identifier.getNumber());
    //        }
    //
    //        // If nothing found or provided hash is not on main chain, return empty
    //        // array
    //        if (startBlock == null) {
    //            return emptyList();
    //        }
    //        if (identifier.getHash() != null) {
    //            IAionBlock mainChainBlock = getBlockByNumber(startBlock.getNumber());
    //            if (!startBlock.equals(mainChainBlock)) {
    //                return emptyList();
    //            }
    //        }
    //
    //        List<A0BlockHeader> headers;
    //        if (skip == 0) {
    //            long bestNumber = bestBlock.getNumber();
    //            headers = getContinuousHeaders(bestNumber, startBlock.getNumber(), limit,
    // reverse);
    //        } else {
    //            headers = getGapedHeaders(startBlock, skip, limit, reverse);
    //        }
    //
    //        return headers;
    //    }
    //
    //    /**
    //     * Finds up to limit blocks starting from blockNumber on main chain
    //     *
    //     * @param bestNumber
    //     *            Number of best block
    //     * @param blockNumber
    //     *            Number of block to start search (included in return)
    //     * @param limit
    //     *            Maximum number of headers in response
    //     * @param reverse
    //     *            Order of search
    //     * @return headers found by query or empty list if none
    //     */
    //    private List<A0BlockHeader> getContinuousHeaders(long bestNumber, long blockNumber, int
    // limit, boolean reverse) {
    //        int qty = getQty(blockNumber, bestNumber, limit, reverse);
    //
    //        byte[] startHash = getStartHash(blockNumber, qty, reverse);
    //
    //        if (startHash == null) {
    //            return emptyList();
    //        }
    //
    //        List<A0BlockHeader> headers = getBlockStore().getListHeadersEndWith(startHash, qty);
    //
    //        // blocks come with falling numbers
    //        if (!reverse) {
    //            Collections.reverse(headers);
    //        }
    //
    //        return headers;
    //    }
    //
    //    /**
    //     * Gets blocks from main chain with gaps between
    //     *
    //     * @param startBlock
    //     *            Block to start from (included in return)
    //     * @param skip
    //     *            Number of blocks skipped between every header in return
    //     * @param limit
    //     *            Maximum number of headers in return
    //     * @param reverse
    //     *            Order of search
    //     * @return headers found by query or empty list if none
    //     */
    //    private List<A0BlockHeader> getGapedHeaders(IAionBlock startBlock, int skip, int limit,
    // boolean reverse) {
    //        List<A0BlockHeader> headers = new ArrayList<>();
    //        headers.add(startBlock.getHeader());
    //        int offset = skip + 1;
    //        if (reverse) {
    //            offset = -offset;
    //        }
    //        long currentNumber = startBlock.getNumber();
    //        boolean finished = false;
    //
    //        while (!finished && headers.size() < limit) {
    //            currentNumber += offset;
    //            IAionBlock nextBlock = getBlockStore().getChainBlockByNumber(currentNumber);
    //            if (nextBlock == null) {
    //                finished = true;
    //            } else {
    //                headers.add(nextBlock.getHeader());
    //            }
    //        }
    //
    //        return headers;
    //    }
    //
    //
    //    private int getQty(long blockNumber, long bestNumber, int limit, boolean reverse) {
    //        if (reverse) {
    //            return blockNumber - limit + 1 < 0 ? (int) (blockNumber + 1) : limit;
    //        } else {
    //            if (blockNumber + limit - 1 > bestNumber) {
    //                return (int) (bestNumber - blockNumber + 1);
    //            } else {
    //                return limit;
    //            }
    //        }
    //    }
    //
    //    private byte[] getStartHash(long blockNumber, int qty, boolean reverse) {
    //
    //        long startNumber;
    //
    //        if (reverse) {
    //            startNumber = blockNumber;
    //        } else {
    //            startNumber = blockNumber + qty - 1;
    //        }
    //
    //        IAionBlock block = getBlockByNumber(startNumber);
    //
    //        if (block == null) {
    //            return null;
    //        }
    //
    //        return block.getHash();
    //    }

    private void updateBestKnownBlock(Block block) {
        updateBestKnownBlock(block.getHeader());
    }

    private void updateBestKnownBlock(BlockHeader header) {
        if (bestKnownBlock.get() == null || header.getNumber() > bestKnownBlock.get().getNumber()) {
            bestKnownBlock.set(new BlockIdentifier(header.getHash(), header.getNumber()));
        }
    }

    public IEventMgr getEventMgr() {
        return this.evtMgr;
    }

    public synchronized boolean recoverWorldState(Repository repository, Block block) {
        if (block == null) {
            LOG.error("World state recovery attempted with null block.");
            return false;
        }
        if (repository.isSnapshot()) {
            LOG.error("World state recovery attempted with snapshot repository.");
            return false;
        }

        long blockNumber = block.getNumber();
        LOG.info(
                "Pruned or corrupt world state at block hash: {}, number: {}."
                        + " Looking for ancestor block with valid world state ...",
                block.getShortHash(),
                blockNumber);

        AionRepositoryImpl repo = (AionRepositoryImpl) repository;

        // keeping track of the original root
        byte[] originalRoot = repo.getRoot();

        Deque<Block> dirtyBlocks = new ArrayDeque<>();
        // already known to be missing the state
        dirtyBlocks.push(block);

        Block other = block;

        // find all the blocks missing a world state
        do {
            other = repo.getBlockStore().getBlockByHash(other.getParentHash());

            // cannot recover if no valid states exist (must build from genesis)
            if (other == null) {
                return false;
            } else {
                dirtyBlocks.push(other);
            }
        } while (!repo.isValidRoot(other.getStateRoot()) && other.getNumber() > 0);

        if (other.getNumber() == 0 && !repo.isValidRoot(other.getStateRoot())) {
            LOG.info("Rebuild state FAILED because a valid state could not be found.");
            return false;
        }

        // sync to the last correct state
        repo.syncToRoot(other.getStateRoot());

        // remove the last added block because it has a correct world state
        dirtyBlocks.pop();

        LOG.info(
                "Valid state found at block hash: {}, number: {}.",
                other.getShortHash(),
                other.getNumber());

        // rebuild world state for dirty blocks
        while (!dirtyBlocks.isEmpty()) {
            other = dirtyBlocks.pop();
            LOG.info(
                    "Rebuilding block hash: {}, number: {}, txs: {}.",
                    other.getShortHash(),
                    other.getNumber(),
                    other.getTransactionsList().size());

            // Load bestblock for executing the CLI command.
            if (bestBlock == null) {
                bestBlock = getBlockStore().getBestBlock();

                if (bestBlock instanceof AionBlock) {
                    bestMiningBlock = (AionBlock) bestBlock;
                } else if (bestBlock instanceof StakingBlock) {
                    bestStakingBlock = (StakingBlock) bestBlock;
                } else {
                    throw new IllegalStateException("Invalid best block!");
                }
            }

            this.add(other, true);
        }

        // update the repository
        repo.flush();

        // setting the root back to its correct value
        repo.syncToRoot(originalRoot);

        // return a flag indicating if the recovery worked
        return repo.isValidRoot(block.getStateRoot());
    }

    public synchronized boolean recoverIndexEntry(Repository repository, Block block) {
        if (block == null) {
            LOG.error("Index recovery attempted with null block.");
            return false;
        }
        if (repository.isSnapshot()) {
            LOG.error("Index recovery attempted with snapshot repository.");
            return false;
        }

        LOG.info(
                "Missing index at block hash: {}, number: {}. Looking for ancestor block with valid index ...",
                block.getShortHash(),
                block.getNumber());

        AionRepositoryImpl repo = (AionRepositoryImpl) repository;

        Deque<Block> dirtyBlocks = new ArrayDeque<>();
        // already known to be missing the state
        dirtyBlocks.push(block);

        Block other = block;

        // find all the blocks missing a world state
        do {
            other = repo.getBlockStore().getBlockByHash(other.getParentHash());

            // cannot recover if no valid states exist (must build from genesis)
            if (other == null) {
                return false;
            } else {
                dirtyBlocks.push(other);
            }
        } while (!repo.isIndexed(other.getHash(), other.getNumber()) && other.getNumber() > 0);

        if (other.getNumber() == 0 && !repo.isIndexed(other.getHash(), other.getNumber())) {
            LOG.info("Rebuild index FAILED because a valid index could not be found.");
            return false;
        }

        // if the size key is missing we set it to the MAX(best block, this block, current value)
        long maxNumber = getBlockStore().getMaxNumber();
        if (bestBlock != null && bestBlock.getNumber() > maxNumber) {
            maxNumber = bestBlock.getNumber();
        }
        if (block.getNumber() > maxNumber) {
            maxNumber = block.getNumber();
        }
        getBlockStore().correctSize(maxNumber, LOG);

        // remove the last added block because it has a correct world state
        Block parentBlock = repo.getBlockStore().getBlockByHashWithInfo(dirtyBlocks.pop().getHash());
        
        LOG.info(
                "Valid index found at block hash: {}, number: {}.",
                parentBlock.getShortHash(),
                parentBlock.getNumber());

        // rebuild world state for dirty blocks
        while (!dirtyBlocks.isEmpty()) {
            other = dirtyBlocks.pop();
            LOG.info(
                    "Rebuilding index for block hash: {}, number: {}, txs: {}.",
                    other.getShortHash(),
                    other.getNumber(),
                    other.getTransactionsList().size());
            parentBlock = repo.getBlockStore().correctIndexEntry(other, parentBlock.getMiningDifficulty(), parentBlock.getStakingDifficulty());
        }
        
        BigInteger totalDiff = parentBlock.getCumulativeDifficulty();

        // update the repository
        repo.flush();

        // return a flag indicating if the recovery worked
        if (repo.isIndexed(block.getHash(), block.getNumber())) {
            Block mainChain = getBlockStore().getBestBlock();
            BigInteger mainChainTotalDiff =
                    getBlockStore().getTotalDifficultyForHash(mainChain.getHash());

            // check if the main chain needs to be updated
            if (mainChainTotalDiff.compareTo(totalDiff) < 0) {
                if (LOG.isInfoEnabled()) {
                    LOG.info(
                            "branching: from = {}/{}, to = {}/{}",
                            mainChain.getNumber(),
                            toHexString(mainChain.getHash()),
                            block.getNumber(),
                            toHexString(block.getHash()));
                }
                getBlockStore().reBranch(block);
                repo.syncToRoot(block.getStateRoot());
                repo.flush();
            } else {
                if (mainChain.getNumber() > block.getNumber()) {
                    // checking if the current recovered blocks are a subsection of the main chain
                    Block ancestor = getBlockByNumber(block.getNumber() + 1);
                    if (ancestor != null
                            && Arrays.equals(ancestor.getParentHash(), block.getHash())) {
                        getBlockStore().correctMainChain(block, LOG);
                        repo.flush();
                    }
                }
            }
            return true;
        } else {
            LOG.info("Rebuild index FAILED.");
            return false;
        }
    }

    public BigInteger getTotalDifficultyByHash(Hash256 hash) {
        if (hash == null) {
            throw new NullPointerException();
        }
        return this.getBlockStore().getTotalDifficultyForHash(hash.toBytes());
    }

    /**
     * A method for creating a new staking block template for the external staker.
     *
     * @param pendingTransactions to be added into the block.
     * @param publicKey the staker's public key.
     * @param newSeed the data decide the weight of the sealing difficulty.
     * @see #createNewStakingBlock(Block, List, byte[], byte[])
     * @return staking block template
     */
    public Block createStakingBlockTemplate(
            List<AionTransaction> pendingTransactions, byte[] publicKey, byte[] newSeed) {
        if (pendingTransactions == null || publicKey == null || newSeed == null) {
            throw new NullPointerException();
        }

        return createNewStakingBlock(getBestBlock(), pendingTransactions, newSeed, publicKey);
    }

    public byte[] getSeed() {
        return bestStakingBlock.getSeed();
    }

    public Block getBestBlockWithInfo() {
        return getBlockStore().getBestBlockWithInfo();
    }

    public Block getBlockWithInfoByHash(byte[] hash) {
        return getBlockStore().getBlockByHashWithInfo(hash);
    }

    /**
     * Initialize as per the <a href=
     * "https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom">Initialization-on-demand</a>
     * holder pattern
     */
    private static class Holder {
        static final AionBlockchainImpl INSTANCE = new AionBlockchainImpl();
    }

    private class State {

        AionRepositoryImpl savedRepo = repository;
        Block savedBest = bestBlock;
        AionBlock savedBestMining = bestMiningBlock;
        StakingBlock savedBestStaking = bestStakingBlock;
        UnityDifficulty ud = new UnityDifficulty(unityDifficulty.get());
    }

    /**
     * @implNote this method only can be called by the aionhub for data recovery purpose.
     * @param blk the best block after database recovered or revered.
     */
    void resetPubBestBlock(Block blk) {
        pubBestBlock = blk;
    }
    
    @VisibleForTesting
    public void setUnityForkNumber(long unityForkNumber) {
        LOG.info("Unity enabled at fork number " + unityForkNumber);
        FORK_5_BLOCK_NUMBER = unityForkNumber;
    }

    long getUnityForkNumber() {
        return FORK_5_BLOCK_NUMBER;
    }

    public StakingBlock getCachingStakingBlockTemplate(byte[] hash) {
        if (hash == null) {
            throw new NullPointerException();
        }

        return stakingBlockTemplate.get(ByteArrayWrapper.wrap(hash));
    }


    public boolean putSealedNewStakingBlock(Block block) {
        if (block == null) {
            throw new NullPointerException();
        }

        if (!(block instanceof StakingBlock)) {
            throw new IllegalArgumentException();
        }

        if (block.getHeader().getNumber() != bestBlock.getHeader().getNumber() + 1) {
            LOG.debug("Invalid block number. {}", block.toString());
            return false;
        }

        long timeStamp = block.getTimestamp();

        // Can not submit a future block
        int stakingBlockCandidateTimeout = 3600;
        if (timeStamp > (TimeUtils.currentTimeSecs() + stakingBlockCandidateTimeout)) {
            LOG.debug("Block timestamp exceed the threshold. {}", block.toString());
            return false;
        }

        LinkedHashSet<StakingBlock> blocks = sealednewStakingBlock.get(timeStamp);
        if (blocks == null) {
            LinkedHashSet<StakingBlock> sets = new LinkedHashSet<>();
            sets.add((StakingBlock) block);
            sealednewStakingBlock.put(block.getHeader().getTimestamp(), sets);
            return true;
        } else {
            boolean exist = blocks.add((StakingBlock) block);
            sealednewStakingBlock.put(block.getHeader().getTimestamp(), blocks);
            return exist;
        }
    }

    public boolean isUnityForkEnabled() {
        return bestBlockNumber.get() >= FORK_5_BLOCK_NUMBER;
    }

    public StakingBlock trySealStakingBlock() {
        StakingBlock bestBlock = null;
        List<Long> removeTimeStamp = new ArrayList<>();
        for (Entry<Long, LinkedHashSet<StakingBlock>> e : sealednewStakingBlock.entrySet()) {
            if (e.getKey() <= TimeUtils.currentTimeSecs()) {
                removeTimeStamp.add(e.getKey());
                for (StakingBlock b : e.getValue()) {
                    if (b != null
                        && (b.getHeader().getNumber()
                            == getBestBlock().getHeader().getNumber() + 1)) {
                        ImportResult result = tryToConnect(b);
                        if (result.isBest()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("NewStakingBlock Sealed. {}", b.toString());
                            } else if (LOG.isInfoEnabled()) {
                                LOG.info(
                                    "NewStakingBlock Sealed. blk#:{} hash:{} difficulty:{} txn:{}",
                                    b.getNumber(),
                                    b.getShortHash(),
                                    b.getDifficultyBI(),
                                    b.getTransactionsList().size());
                            }

                            bestBlock = new StakingBlock(b);
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (bestBlock != null) {
            sealednewStakingBlock.clear();
        } else {
            for (Long timeStamp : removeTimeStamp) {
                sealednewStakingBlock.remove(timeStamp);
            }
        }

        return bestBlock;
    }
}
