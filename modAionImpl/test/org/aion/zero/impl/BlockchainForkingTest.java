package org.aion.zero.impl;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.zero.impl.BlockchainTestUtils.generateAccounts;
import static org.aion.zero.impl.BlockchainTestUtils.generateTransactions;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.aion.avm.core.dappreading.JarBuilder;
import org.aion.avm.userlib.CodeAndArguments;
import org.aion.base.AionTransaction;
import org.aion.base.TransactionTypes;
import org.aion.crypto.ECKey;
import org.aion.crypto.HashUtil;
import org.aion.log.AionLoggerFactory;
import org.aion.mcf.core.ImportResult;
import org.aion.mcf.db.InternalVmType;
import org.aion.mcf.valid.TransactionTypeRule;
import org.aion.types.AionAddress;
import org.aion.util.biginteger.BIUtil;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.vm.BlockCachingContext;
import org.aion.vm.LongLivedAvm;
import org.aion.zero.impl.blockchain.ChainConfiguration;
import org.aion.zero.impl.db.ContractInformation;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionBlockSummary;
import org.aion.zero.impl.vm.contracts.AvmHelloWorld;
import org.aion.zero.types.AionTxReceipt;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlockchainForkingTest {

    @Before
    public void setup() {
        // reduce default logging levels
        Map<String, String> cfg = new HashMap<>();
        cfg.put("API", "ERROR");
        cfg.put("CONS", "DEBUG");
        cfg.put("DB", "ERROR");
        cfg.put("GEM", "ERROR");
        cfg.put("P2P", "ERROR");
        cfg.put("ROOT", "ERROR");
        cfg.put("SYNC", "ERROR");
        cfg.put("TX", "DEBUG");
        cfg.put("VM", "DEBUG");
        AionLoggerFactory.init(cfg);

        LongLivedAvm.createAndStartLongLivedAvm();
    }

    @After
    public void shutdown() {
        LongLivedAvm.destroy();
    }
    /*-
     * Tests the case where multiple threads submit a single block (content) but
     * with different mining nonces and solutions. In this case our rules dictate
     * that all subsequent blocks are considered invalid.
     *
     *          (common ancestor)
     *          /               \
     *         /                 \
     *        /                   \
     *       (a)o                 (b)x
     *
     * Given:
     * a.td == b.td
     */
    @Test
    public void testSameBlockDifferentNonceAndSolutionSimple() {
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain.Bundle b = builder.withValidatorConfiguration("simple").build();

        StandaloneBlockchain bc = b.bc;
        AionBlock block = bc.createNewBlock(bc.getBestBlock(), Collections.emptyList(), true);
        AionBlock sameBlock = new AionBlock(block.getEncoded());

        ImportResult firstRes = bc.tryToConnect(block);

        // check that the returned block is the first block
        assertThat(bc.getBestBlock() == block).isTrue();
        assertThat(firstRes).isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        Pair<Long, BlockCachingContext> cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(block.getNumber() - 1);
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);

        ImportResult secondRes = bc.tryToConnect(sameBlock);

        // the second block should get rejected, so check that the reference still refers
        // to the first block (we dont change the published reference)
        assertThat(bc.getBestBlock() == block).isTrue();
        assertThat(secondRes).isEqualTo(ImportResult.EXIST);

        // the caching context does not change for already known blocks
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(block.getNumber() - 1);
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);
    }

    /*-
     * Test the general forking case, where an incoming block (b) has a greater total
     * difficulty than our current block. In this scenario, we should switch to
     * the branch (sub-tree) that has (b) at its tip.
     *
     * This is the simplest case, where the distance between (a) (our current head) and
     * (b) is 2. This implies that the common ancestor is directly adjacent to both blocks.
     *
     *          (common ancestor)
     *          /               \
     *         /                 \
     *        /                   \
     *       (a)x(low td)           (b)o(higher td)
     *
     * In this simple case:
     * b.td > a.td
     * a_worldState === b_worldState
     *
     */
    @Test
    public void testInvalidFirstBlockDifficulty() {
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain.Bundle b = builder.withValidatorConfiguration("simple").build();

        StandaloneBlockchain bc = b.bc;
        AionBlock bestBlock = bc.getBestBlock();
        AionBlock standardBlock =
                bc.createNewBlock(bc.getBestBlock(), Collections.emptyList(), true);

        ChainConfiguration cc = new ChainConfiguration();
        AionBlock higherDifficultyBlock = new AionBlock(standardBlock);
        higherDifficultyBlock.getHeader().setTimestamp(bestBlock.getTimestamp() + 1);

        BigInteger difficulty =
                cc.getDifficultyCalculator()
                        .calculateDifficulty(
                                higherDifficultyBlock.getHeader(), bestBlock.getHeader());

        assertThat(difficulty).isGreaterThan(standardBlock.getDifficultyBI());
        higherDifficultyBlock.getHeader().setDifficulty(difficulty.toByteArray());

        System.out.println(
                "before any processing: " + new ByteArrayWrapper(bc.getRepository().getRoot()));
        System.out.println("trie: " + bc.getRepository().getWorldState().getTrieDump());

        ImportResult result = bc.tryToConnect(standardBlock);
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        Pair<Long, BlockCachingContext> cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(standardBlock.getNumber() - 1);
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);

        // assert that the block we just inserted (best) is the instance that is returned
        assertThat(bc.getBestBlock() == standardBlock).isTrue();

        System.out.println(new ByteArrayWrapper(bc.getRepository().getRoot()));

        ImportResult higherDifficultyResult = bc.tryToConnect(higherDifficultyBlock);

        /**
         * With our updates to difficulty verification and calculation, this block is now invalid
         */
        assertThat(higherDifficultyResult).isEqualTo(ImportResult.INVALID_BLOCK);
        assertThat(bc.getBestBlockHash()).isEqualTo(standardBlock.getHash());

        // since the block is second for that height, it is assumed as sidechain
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(standardBlock.getNumber() - 1);
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.SIDECHAIN);

        // the object reference here is intentional
        assertThat(bc.getBestBlock() == standardBlock).isTrue();

        // check for correct state rollback
        assertThat(bc.getRepository().getRoot()).isEqualTo(standardBlock.getStateRoot());
        assertThat(bc.getTotalDifficulty())
                .isEqualTo(
                        bc.getRepository()
                                .getBlockStore()
                                .getTotalDifficultyForHash(standardBlock.getHash()));
    }

    /*-
     *
     * Recall previous forking logic worked as follows:
     *
     *          [ parent block ]
     *          /              \
     *         /                \
     *        /                  \
     *  [block_1]              [block_2]
     *  TD=101                 TD=102
     *
     * Where if block_1 had a greater timestamp (and thus lower TD) than
     * block_2, then block_2 would be accepted as the best block for
     * that particular level (until later re-orgs prove this to be untrue)
     *
     * With our changes to difficulty calculations, difficulty is calculated
     * with respect to the two previous blocks (parent, grandParent) blocks
     * in the sequence.
     *
     * This leads to the following:
     *
     *          [ parent block - 1] TD = 50
     *                  |
     *                  |
     *          [ parent block ] D = 50
     *          /              \
     *         /                \
     *        /                  \
     *    [block_1]            [block_2]
     *    TD=100               TD=100
     *
     * Where both blocks are guaranteed to have the same TD if they directly
     * branch off of the same parent. In fact, this guarantees us that the
     * first block coming in from the network (for a particular level) is
     * the de-facto best block for a short period of time.
     *
     * It is only when the block after comes in (for both chains) that a re-org
     * will happen on one of the chains (possibly)
     *
     *
     *             ...prev
     *   [block_1]              [block_2]
     *   T(n) = T(n-1) + 4      T(n) = T(n-1) + 20
     *       |                         |
     *       |                         |
     *       |                         |
     *   [block_1_2]            [block_1_2]
     *   TD = 160               TD = 140
     *
     * At which point a re-org should occur on most blocks. Remember that this reorg
     * is a minimum, given equal hashing power and particularily bad luck, two parties
     * could indefinitely stay on their respective chains, but the chances of this is
     * extraordinarily small.
     */
    @Test
    public void testSecondBlockHigherDifficultyFork() {
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain.Bundle bundle =
                builder.withValidatorConfiguration("simple").withDefaultAccounts().build();

        long time = System.currentTimeMillis();

        StandaloneBlockchain bc = bundle.bc;

        // generate three blocks, on the third block we get flexibility
        // for what difficulties can occur

        AionBlock firstBlock =
                bc.createNewBlockInternal(
                                bc.getGenesis(), Collections.emptyList(), true, time / 1000L)
                        .block;
        assertThat(bc.tryToConnectInternal(firstBlock, (time += 10)))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        Pair<Long, BlockCachingContext> cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(firstBlock.getNumber() - 1);
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);

        // now connect the second block
        AionBlock secondBlock =
                bc.createNewBlockInternal(firstBlock, Collections.emptyList(), true, time / 1000L)
                        .block;
        assertThat(bc.tryToConnectInternal(secondBlock, time += 10))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(firstBlock.getNumber()); // the parent
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);

        // now on the third block, we diverge with one block having higher TD than the other
        AionBlock fasterSecondBlock =
                bc.createNewBlockInternal(secondBlock, Collections.emptyList(), true, time / 1000L)
                        .block;
        AionBlock slowerSecondBlock = new AionBlock(fasterSecondBlock);

        slowerSecondBlock.getHeader().setTimestamp(time / 1000L + 100);

        assertThat(bc.tryToConnectInternal(fasterSecondBlock, time + 100))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(secondBlock.getNumber()); // the parent
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);

        assertThat(bc.tryToConnectInternal(slowerSecondBlock, time + 100))
                .isEqualTo(ImportResult.IMPORTED_NOT_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(secondBlock.getNumber()); // the parent
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.SIDECHAIN);

        // represents the amount of time we would have waited for the lower TD block to come in
        long timeDelta = 1000L;

        // loweredDifficulty = bi - bi / 1024
        BigInteger loweredDifficulty =
                BIUtil.max(
                        secondBlock
                                .getDifficultyBI()
                                .subtract(
                                        secondBlock
                                                .getDifficultyBI()
                                                .divide(BigInteger.valueOf(1024L))),
                        BigInteger.valueOf(16L));

        time += 100;

        AionBlock fastBlockDescendant =
                bc.createNewBlockInternal(
                                fasterSecondBlock, Collections.emptyList(), true, time / 1000L)
                        .block;
        AionBlock slowerBlockDescendant =
                bc.createNewBlockInternal(
                                slowerSecondBlock,
                                Collections.emptyList(),
                                true,
                                time / 1000L + 100 + 1)
                        .block;

        // increment by another hundred (this is supposed to be when the slower block descendant is
        // completed)
        time += 100;

        assertThat(fastBlockDescendant.getDifficultyBI())
                .isGreaterThan(slowerBlockDescendant.getDifficultyBI());
        System.out.println("faster block descendant TD: " + fastBlockDescendant.getDifficultyBI());
        System.out.println(
                "slower block descendant TD: " + slowerBlockDescendant.getDifficultyBI());

        assertThat(bc.tryToConnectInternal(slowerBlockDescendant, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(0); // no known parent
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.DEEP_SIDECHAIN);

        assertThat(bc.tryToConnectInternal(fastBlockDescendant, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(0); // parent had been made side chain
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.DEEP_SIDECHAIN);

        assertThat(bc.getBestBlock()).isEqualTo(fastBlockDescendant);

        // ensuring that the caching is correct for the nest block to be added
        AionBlock switchBlock =
                bc.createNewBlockInternal(
                                fastBlockDescendant, Collections.emptyList(), true, time / 1000L)
                        .block;

        assertThat(bc.tryToConnectInternal(switchBlock, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(secondBlock.getNumber()); // common ancestor
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.SWITCHING_MAINCHAIN);

        // ensuring that the caching is correct for the nest block to be added
        AionBlock lastBlock =
                bc.createNewBlockInternal(switchBlock, Collections.emptyList(), true, time / 1000L)
                        .block;

        assertThat(bc.tryToConnectInternal(lastBlock, time)).isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the correct caching context was used
        cacheContext = bc.getAvmCachingContext();
        assertThat(cacheContext.getLeft()).isEqualTo(switchBlock.getNumber()); // parent
        assertThat(cacheContext.getRight()).isEqualTo(BlockCachingContext.MAINCHAIN);
    }

    /** Test fork with exception. */
    @Test
    public void testSecondBlockHigherDifficultyFork_wExceptionOnFasterBlockAdd() {
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain.Bundle bundle =
                builder.withValidatorConfiguration("simple").withDefaultAccounts().build();

        long time = System.currentTimeMillis();

        StandaloneBlockchain bc = bundle.bc;

        // generate three blocks, on the third block we get flexibility
        // for what difficulties can occur

        BlockContext firstBlock =
                bc.createNewBlockInternal(
                        bc.getGenesis(), Collections.emptyList(), true, time / 1000L);
        assertThat(bc.tryToConnectInternal(firstBlock.block, (time += 10)))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // now connect the second block
        BlockContext secondBlock =
                bc.createNewBlockInternal(
                        firstBlock.block, Collections.emptyList(), true, time / 1000L);
        assertThat(bc.tryToConnectInternal(secondBlock.block, time += 10))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // now on the third block, we diverge with one block having higher TD than the other
        BlockContext fasterSecondBlock =
                bc.createNewBlockInternal(
                        secondBlock.block, Collections.emptyList(), true, time / 1000L);
        AionBlock slowerSecondBlock = new AionBlock(fasterSecondBlock.block);

        slowerSecondBlock.getHeader().setTimestamp(time / 1000L + 100);

        assertThat(bc.tryToConnectInternal(fasterSecondBlock.block, time + 100))
                .isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(bc.tryToConnectInternal(slowerSecondBlock, time + 100))
                .isEqualTo(ImportResult.IMPORTED_NOT_BEST);

        time += 100;

        BlockContext fastBlockDescendant =
                bc.createNewBlockInternal(
                        fasterSecondBlock.block, Collections.emptyList(), true, time / 1000L);
        BlockContext slowerBlockDescendant =
                bc.createNewBlockInternal(
                        slowerSecondBlock, Collections.emptyList(), true, time / 1000L + 100 + 1);

        // increment by another hundred (this is supposed to be when the slower block descendant is
        // completed)
        time += 100;

        assertThat(fastBlockDescendant.block.getDifficultyBI())
                .isGreaterThan(slowerBlockDescendant.block.getDifficultyBI());
        System.out.println(
                "faster block descendant TD: " + fastBlockDescendant.block.getDifficultyBI());
        System.out.println(
                "slower block descendant TD: " + slowerBlockDescendant.block.getDifficultyBI());

        assertThat(bc.tryToConnectInternal(slowerBlockDescendant.block, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // corrupt the parent for the fast block descendant
        bc.getRepository().getStateDatabase().delete(fasterSecondBlock.block.getStateRoot());
        assertThat(bc.getRepository().isValidRoot(fasterSecondBlock.block.getStateRoot()))
                .isFalse();

        // attempt adding the fastBlockDescendant
        assertThat(bc.tryToConnectInternal(fastBlockDescendant.block, time))
                .isEqualTo(ImportResult.INVALID_BLOCK);

        // check for correct state rollback
        assertThat(bc.getBestBlock()).isEqualTo(slowerBlockDescendant.block);
        assertThat(bc.getRepository().getRoot())
                .isEqualTo(slowerBlockDescendant.block.getStateRoot());
        assertThat(bc.getTotalDifficulty())
                .isEqualTo(
                        bc.getRepository()
                                .getBlockStore()
                                .getTotalDifficultyForHash(slowerBlockDescendant.block.getHash()));
    }

    @Test
    public void testRollbackWithAddInvalidBlock() {
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain.Bundle b = builder.withValidatorConfiguration("simple").build();
        StandaloneBlockchain bc = b.bc;
        AionBlock block = bc.createNewBlock(bc.getBestBlock(), Collections.emptyList(), true);

        assertThat(bc.tryToConnect(block)).isEqualTo(ImportResult.IMPORTED_BEST);

        // check that the returned block is the first block
        assertThat(bc.getBestBlock() == block).isTrue();

        AionBlock invalidBlock =
                bc.createNewBlock(bc.getBestBlock(), Collections.emptyList(), true);
        invalidBlock.getHeader().setDifficulty(BigInteger.ONE.toByteArray());

        // attempting to add invalid block
        assertThat(bc.tryToConnect(invalidBlock)).isEqualTo(ImportResult.INVALID_BLOCK);

        // check for correct state rollback
        assertThat(bc.getBestBlock()).isEqualTo(block);
        assertThat(bc.getRepository().getRoot()).isEqualTo(block.getStateRoot());
        assertThat(bc.getTotalDifficulty())
                .isEqualTo(
                        bc.getRepository()
                                .getBlockStore()
                                .getTotalDifficultyForHash(block.getHash()));
    }

    /**
     * Test the fork case when the block being replaced had contract storage changes that differ
     * from the previous block and are replaced by new changes in the updated block.
     *
     * <p>Ensures that the output of applying the block after the fork is the same as applying the
     * block first.
     */
    @Test
    public void testForkWithRevertOnSmallContractStorage() {

        // ****** setup ******

        // build a blockchain with CONCURRENT_THREADS_PER_TYPE blocks
        List<ECKey> accounts = generateAccounts(10);
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain sourceChain =
                builder.withValidatorConfiguration("simple")
                        .withDefaultAccounts(accounts)
                        .build()
                        .bc;
        StandaloneBlockchain testChain =
                builder.withValidatorConfiguration("simple")
                        .withDefaultAccounts(accounts)
                        .build()
                        .bc;

        assertThat(testChain).isNotEqualTo(sourceChain);
        assertThat(testChain.genesis).isEqualTo(sourceChain.genesis);

        long time = System.currentTimeMillis();

        // add a block with contract deploy
        ECKey sender = accounts.remove(0);
        AionTransaction deployTx = deployContract(sender);

        AionBlock block =
                sourceChain.createNewBlockInternal(
                                sourceChain.genesis, Arrays.asList(deployTx), true, time / 10_000L)
                        .block;

        Pair<ImportResult, AionBlockSummary> connectResult =
                sourceChain.tryToConnectAndFetchSummary(block, (time += 10), true);
        AionTxReceipt receipt = connectResult.getRight().getReceipts().get(0);
        assertThat(receipt.isSuccessful()).isTrue();

        ImportResult result = connectResult.getLeft();
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        result = testChain.tryToConnectInternal(block, time);
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        assertThat(testChain.getRepository().getRoot())
                .isEqualTo(sourceChain.getRepository().getRoot());

        AionAddress contract = receipt.getTransaction().getContractAddress();
        // add a block with transactions to both
        List<AionTransaction> txs = generateTransactions(20, accounts, sourceChain.getRepository());

        block =
                sourceChain.createNewBlockInternal(
                                sourceChain.getBestBlock(), txs, true, time / 10_000L)
                        .block;

        result = sourceChain.tryToConnectInternal(block, (time += 10));
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        result = testChain.tryToConnectInternal(block, time);
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        assertThat(testChain.getRepository().getRoot())
                .isEqualTo(sourceChain.getRepository().getRoot());

        // create a slow / fast block distinction
        AionTransaction callTx = callSetValue2(sender, contract, 5, 6, BigInteger.ONE);
        AionBlock fastBlock =
                sourceChain.createNewBlockInternal(
                                sourceChain.getBestBlock(),
                                Arrays.asList(callTx),
                                true,
                                time / 10_000L)
                        .block;

        callTx = callSetValue2(sender, contract, 1, 9, BigInteger.ONE);
        AionBlock slowBlock =
                new AionBlock(
                        sourceChain.createNewBlockInternal(
                                        sourceChain.getBestBlock(),
                                        Arrays.asList(callTx),
                                        true,
                                        time / 10_000L)
                                .block);

        slowBlock.getHeader().setTimestamp(time / 10_000L + 100);

        time += 100;

        // sourceChain imports only fast block
        assertThat(sourceChain.tryToConnectInternal(fastBlock, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // testChain imports both blocks
        assertThat(testChain.tryToConnectInternal(fastBlock, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(testChain.tryToConnectInternal(slowBlock, time))
                .isEqualTo(ImportResult.IMPORTED_NOT_BEST);

        // build two blocks with different contract storage calls
        // the second block gets a higher total difficulty
        callTx = callSetValue(sender, contract, 5, BigInteger.TWO);

        AionBlock lowBlock =
                testChain.createNewBlockInternal(
                                slowBlock, Arrays.asList(callTx), true, time / 10_000L + 101)
                        .block;

        callTx = callSetValue(sender, contract, 9, BigInteger.TWO);
        AionBlock highBlock =
                sourceChain.createNewBlockInternal(
                                fastBlock, Arrays.asList(callTx), true, time / 10_000L)
                        .block;

        // System.out.println("***highBlock TD: " + highBlock.getDifficultyBI());
        // System.out.println("***lowBlock TD: " + lowBlock.getDifficultyBI());
        assertThat(highBlock.getDifficultyBI()).isGreaterThan(lowBlock.getDifficultyBI());

        time += 100;

        // build first chain with highBlock applied directly
        connectResult = sourceChain.tryToConnectAndFetchSummary(highBlock, time, true);
        receipt = connectResult.getRight().getReceipts().get(0);
        assertThat(receipt.isSuccessful()).isTrue();

        result = connectResult.getLeft();
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        // collect the consensus information from the block & receipt
        AionBlockSummary blockSummary = connectResult.getRight();
        byte[] stateRoot = blockSummary.getBlock().getStateRoot();
        byte[] blockReceiptsRoot = blockSummary.getBlock().getReceiptsRoot();
        byte[] receiptTrieEncoded = receipt.getReceiptTrieEncoded();

        // ****** test fork behavior ******

        // first import lowBlock
        assertThat(testChain.tryToConnectInternal(lowBlock, time))
                .isEqualTo(ImportResult.IMPORTED_BEST);

        // next import highBlock causing the fork
        connectResult = testChain.tryToConnectAndFetchSummary(highBlock, time, true);
        receipt = connectResult.getRight().getReceipts().get(0);
        assertThat(receipt.isSuccessful()).isTrue();
        System.out.println(receipt);

        result = connectResult.getLeft();
        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);

        // collect the consensus information from the block & receipt
        blockSummary = connectResult.getRight();
        assertThat(testChain.getBestBlock()).isEqualTo(sourceChain.getBestBlock());
        assertThat(blockSummary.getBlock().getStateRoot()).isEqualTo(stateRoot);
        assertThat(blockSummary.getBlock().getReceiptsRoot()).isEqualTo(blockReceiptsRoot);
        assertThat(receipt.getReceiptTrieEncoded()).isEqualTo(receiptTrieEncoded);
    }

    private AionTransaction deployContract(ECKey sender) {
        // contract source code for reference
        /*
        pragma solidity ^0.4.15;
        contract Storage {
            uint128 value;
            mapping(uint128 => uint128) private userPrivilege;
            struct Entry {
                uint128 id;
                uint128 value;
            }
            Entry value2;
            function Storage(){
            value = 10;
            userPrivilege[value] = value;
            value2.id = 100;
            value2.value = 200;
            userPrivilege[value2.id] = value2.value;
            }
            function setValue(uint128 newValue)  {
            value = newValue;
            userPrivilege[newValue] = newValue+1;
            }
            function setValue2(uint128 v1, uint128 v2)  {
            value2.id = v1;
            value2.value = v2;
            userPrivilege[v1] = v1+v2;
            }
            function getValue() returns(uint)  {
            return value;
            }
        }
        */

        // code for contract
        String contractCode =
                "0x605060405234156100105760006000fd5b5b600a600060005081909090555060006000505460016000506000600060005054815260100190815260100160002090506000508190909055506064600260005060000160005081909090555060c8600260005060010160005081909090555060026000506001016000505460016000506000600260005060000160005054815260100190815260100160002090506000508190909055505b6100ae565b610184806100bd6000396000f30060506040526000356c01000000000000000000000000900463ffffffff1680631677b0ff14610049578063209652551461007657806362eb702a146100a057610043565b60006000fd5b34156100555760006000fd5b61007460048080359060100190919080359060100190919050506100c4565b005b34156100825760006000fd5b61008a610111565b6040518082815260100191505060405180910390f35b34156100ac5760006000fd5b6100c26004808035906010019091905050610123565b005b8160026000506000016000508190909055508060026000506001016000508190909055508082016001600050600084815260100190815260100160002090506000508190909055505b5050565b60006000600050549050610120565b90565b806000600050819090905550600181016001600050600083815260100190815260100160002090506000508190909055505b505600a165627a7a723058205b6e690d70d3703337452467437dc7c4e863ee4ad34b24cc516e2afa71e334700029";

        AionTransaction contractDeploymentTx =
                new AionTransaction(
                        BigInteger.ZERO.toByteArray(),
                        null,
                        BigInteger.ZERO.toByteArray(),
                        ByteUtil.hexStringToBytes(contractCode),
                        5_000_000L,
                        10_123_456_789L);

        contractDeploymentTx.sign(sender);

        return contractDeploymentTx;
    }

    private AionTransaction callSetValue(
            ECKey sender, AionAddress contract, int digit, BigInteger nonce) {
        // calls setValue(digit)
        if (digit < 0 || digit > 9) {
            return null; // should actually be a digit
        }
        // code for contract call
        String contractCode = "62eb702a0000000000000000000000000000000" + digit;

        AionTransaction contractCallTx =
                new AionTransaction(
                        nonce.toByteArray(),
                        contract,
                        BigInteger.ZERO.toByteArray(),
                        Hex.decode(contractCode),
                        2_000_000L,
                        10_123_456_789L);

        contractCallTx.sign(sender);

        return contractCallTx;
    }

    private AionTransaction callSetValue2(
            ECKey sender, AionAddress contract, int digit1, int digit2, BigInteger nonce) {
        // calls setValue2(digit, digit)
        if (digit1 < 0 || digit1 > 9 || digit2 < 0 || digit2 > 9) {
            return null; // should actually be a digit
        }
        // code for contract call
        String contractCode =
                "1677b0ff0000000000000000000000000000000"
                        + digit1
                        + "0000000000000000000000000000000"
                        + digit2;

        AionTransaction contractCallTx =
                new AionTransaction(
                        nonce.toByteArray(),
                        contract,
                        BigInteger.ZERO.toByteArray(),
                        Hex.decode(contractCode),
                        2_000_000L,
                        10_123_456_789L);

        contractCallTx.sign(sender);

        return contractCallTx;
    }

    /** TODO */
    @Test
    public void testVmTypeRetrieval_ImportSideChainWithConflictingContractVMAndSameCode() {
        // blocks to be built
        AionBlock block, fastBlock, slowBlock, highBlock, lowBlock;

        // transactions used in blocks
        AionTransaction deployOnAVM, deployOnFVM, callAVM, callFVM;
        List<AionTransaction> txs;

        // for processing block results
        Pair<ImportResult, AionBlockSummary> connectResult;
        ImportResult result;
        AionTxReceipt receipt;

        // build a blockchain
        TransactionTypeRule.allowAVMContractTransaction();
        List<ECKey> accounts = generateAccounts(10);
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain sourceChain =
                builder.withValidatorConfiguration("simple")
                        .withDefaultAccounts(accounts)
                        .build()
                        .bc;
        StandaloneBlockchain testChain =
                builder.withValidatorConfiguration("simple")
                        .withDefaultAccounts(accounts)
                        .build()
                        .bc;
        ECKey sender = accounts.remove(0);

        assertThat(testChain).isNotEqualTo(sourceChain);
        assertThat(testChain.genesis).isEqualTo(sourceChain.genesis);

        long time = System.currentTimeMillis();

        // ****** setup side chain ******

        byte[] avmHelloWorldCode =
                ByteUtil.hexStringToBytes(
                        "00000737504b0304140008080800d4ad282b000000000000000000000000140004004d4554412d494e462f4d414e49464553542e4d46feca0000f34dcccb4c4b2d2ed10d4b2d2acecccfb35230d433e0e5f24dccccd375ce492c2eb65270e4e5e2e50200504b07082bc2bf352a00000028000000504b0304140008080800d4ad282b00000000000000000000000007000000412e636c61737365905d4f135110869fd342cf765d28b4a0f8ddfab94b89bdf306636235c628c68b1a087275ba6ceac26117ca42c24ff15fe80d366abc35f13f699cb398187592397366de77e69d73befffcf415b8cf8a423dd228c5dc8e39363d6bb251efd57027890b4d55d17896589b6fe463bb7dcf11842e5e0da375455062ed12ec78788a5973bcd7ebdb3cde8ddf9a34d3f80abd3f4eb3c2668ac570ed8fc6a090f268355a0f0898f139c7ac626a4fba2484d1565f94fa9aa60c1825c51353988005e61d7151517b906669f150311d6ef5dd8c0b2cf9b4b8285d43c54218fd2f1570992b8e74f5afc79ea19aeb0aefd09c946ff2e88846727064ece13f7b9ffdcd6af426e026b77cdadc0ea8a17d2adc95d51fe7db896266509878f7a5d97f6d8656727f901f8de3e4696a133ac29cc25985ba6b451149d694e8acd6ac4f687c28ef75e698ff8d3fa72a1db0f299d6e684f31fb9f462f9946b6bdd6fe8ea17ef7db779e3943beff027842e772e7cc5b29c01951f2c695a9a76436add527ffa17504b070871e5b2737c01000009020000504b0304140008080800d4ad282b00000000000000000000000007000000422e636c6173739d54dd4e134114fea6ddd2525b29c87f0b0e8ada1f290882220514880949c50b080972c3b63b85e2b24bb60ba18931be030f203131dc780189881183f7be82af42c433dbd216f58aa6bb737ebff39d3333fbf3f7b753002378cac0a6bc600ca175755bedd75563b5ff65665d646d2fdc0c0dcfa6666744d6d484959401142e9f5937659f939091828f842c836b798a048da12e9537f2f6048327ba3c155b647047638b015c47c80f058d015c43a01e2edc082008af945a189aa3b17495c2bc6de58dd53119d0e6a780f64b044b5e2f3a1de879aa2a02e892f02e7493960b8097b41ed2a6bdb8cd9098133b3617bad81086cdf306d7545be5b9bcd0359e2f70c3b4b9ca0b0e70d2873b0c2dd17ff994dab8e7472fa20ca9992a84668a12c89aba2db830ccadd5359e29da64d545cee6b6c92da16ab535e2721011c9f23ec3e415b0ec35225e814b5e1e51b1608b0d2f0618ea55cb528b5973b3c810af6daab4cf63b3ff31cdca4e0731e4c7033c943d37f811c608c3c08295179aa4a039c782e72c7383ba32b6749d9b16171b9b76b166b644ec31c3f855266570d36143104f1882d3a651b055c35e54f52dc1a04c537532cfdb6af6f50b757341cde8a4fbe7cd2d2b2b9ee775811e1aad42e7534127ea40a71c29d25cf0915e5fa3cb2da023e9c8742069ad231ff54cef71d2c26477d1da103f46533c7184e6b8fb08ad87903f9a2fdaca81a3e5c02e16ff8ce68f08b8e57aba473724bd0f5ffc0b3ad28903276982de7eb81b9533285ea7029d850a8ce2c0f4c96c7a5a8742be9ddd54f8425556861a71fef6dd6e2abe44aa6785c8843f95d9042b6c7ec1e3b47f40b46fd2738ba5ab80837be03578a19e3d84aa68fb78e338a5e195222d14f11ec113f42e35dd3d46ec87d3984cd63c1fcaf684633f46df78d519b970f64b67e4bb6feea2689f3bf215c34e85882c7082f052df311ed50ea8039e330c7bbadbce1061e774e95c5ef90f3b3b45f7bddce820dc248162fe1a7ba9f4a82c7d5801a5d936ca6f0eafe427cbf91dff696aec723205b69338e930f0fc01504b0708c9717be2f502000047050000504b0304140008080800d4ad282b00000000000000000000000007000000432e636c617373458ebb0ac2401045efc61825be059b74763ec074368aa04141b13260bfe81256e2aec48df85b5682851fe04789a3820ecceb72e7308fe7ed0ea08f1a030b72600cde8e9fb81f7315f9ab5419b917d3f3461c8cd42a870c436d3c99ff94dedb4ca7827291c53708e20ca59266c4d0682dffbcd024524583f6ba08170517368a0ca540aba3e1caac799c12c70ef4969a1bea34d988998c059ab0c84b607870405fa24c9b853c4dec8da25a21a54e9a45dde974af285dbecfa0faf1665f504b0708cf75737cbc000000eb000000504b01021400140008080800d4ad282b2bc2bf352a000000280000001400040000000000000000000000000000004d4554412d494e462f4d414e49464553542e4d46feca0000504b01021400140008080800d4ad282b71e5b2737c01000009020000070000000000000000000000000070000000412e636c617373504b01021400140008080800d4ad282bc9717be2f502000047050000070000000000000000000000000021020000422e636c617373504b01021400140008080800d4ad282bcf75737cbc000000eb00000007000000000000000000000000004b050000432e636c617373504b05060000000004000400e50000003c0600000000");
        byte[] avmTransformedHelloWorldCode =
                ByteUtil.hexStringToBytes(
                        "504b0304140008080800d4ad282b000000000000000000000000140004004d4554412d494e462f4d414e49464553542e4d46feca0000f34dcccb4c4b2d2ed10d4b2d2acecccfb35230d433e0e5f24dccccd375ce492c2eb65270e4e5e2e50200504b07082bc2bf352a00000028000000504b0304140008080800d4ad282b00000000000000000000000007000000412e636c61737365905d4f135110869fd342cf765d28b4a0f8ddfab94b89bdf306636235c628c68b1a087275ba6ceac26117ca42c24ff15fe80d366abc35f13f699cb398187592397366de77e69d73befffcf415b8cf8a423dd228c5dc8e39363d6bb251efd57027890b4d55d17896589b6fe463bb7dcf11842e5e0da375455062ed12ec78788a5973bcd7ebdb3cde8ddf9a34d3f80abd3f4eb3c2668ac570ed8fc6a090f268355a0f0898f139c7ac626a4fba2484d1565f94fa9aa60c1825c51353988005e61d7151517b906669f150311d6ef5dd8c0b2cf9b4b8285d43c54218fd2f1570992b8e74f5afc79ea19aeb0aefd09c946ff2e88846727064ece13f7b9ffdcd6af426e026b77cdadc0ea8a17d2adc95d51fe7db896266509878f7a5d97f6d8656727f901f8de3e4696a133ac29cc25985ba6b451149d694e8acd6ac4f687c28ef75e698ff8d3fa72a1db0f299d6e684f31fb9f462f9946b6bdd6fe8ea17ef7db779e3943beff027842e772e7cc5b29c01951f2c695a9a76436add527ffa17504b070871e5b2737c01000009020000504b0304140008080800d4ad282b00000000000000000000000007000000422e636c6173739d54dd4e134114fea6ddd2525b29c87f0b0e8ada1f290882220514880949c50b080972c3b63b85e2b24bb60ba18931be030f203131dc780189881183f7be82af42c433dbd216f58aa6bb737ebff39d3333fbf3f7b753002378cac0a6bc600ca175755bedd75563b5ff65665d646d2fdc0c0dcfa6666744d6d484959401142e9f5937659f939091828f842c836b798a048da12e9537f2f6048327ba3c155b647047638b015c47c80f058d015c43a01e2edc082008af945a189aa3b17495c2bc6de58dd53119d0e6a780f64b044b5e2f3a1de879aa2a02e892f02e7493960b8097b41ed2a6bdb8cd9098133b3617bad81086cdf306d7545be5b9bcd0359e2f70c3b4b9ca0b0e70d2873b0c2dd17ff994dab8e7472fa20ca9992a84668a12c89aba2db830ccadd5359e29da64d545cee6b6c92da16ab535e2721011c9f23ec3e415b0ec35225e814b5e1e51b1608b0d2f0618ea55cb528b5973b3c810af6daab4cf63b3ff31cdca4e0731e4c7033c943d37f811c608c3c08295179aa4a039c782e72c7383ba32b6749d9b16171b9b76b166b644ec31c3f855266570d36143104f1882d3a651b055c35e54f52dc1a04c537532cfdb6af6f50b757341cde8a4fbe7cd2d2b2b9ee775811e1aad42e7534127ea40a71c29d25cf0915e5fa3cb2da023e9c8742069ad231ff54cef71d2c26477d1da103f46533c7184e6b8fb08ad87903f9a2fdaca81a3e5c02e16ff8ce68f08b8e57aba473724bd0f5ffc0b3ad28903276982de7eb81b9533285ea7029d850a8ce2c0f4c96c7a5a8742be9ddd54f8425556861a71fef6dd6e2abe44aa6785c8843f95d9042b6c7ec1e3b47f40b46fd2738ba5ab80837be03578a19e3d84aa68fb78e338a5e195222d14f11ec113f42e35dd3d46ec87d3984cd63c1fcaf684633f46df78d519b970f64b67e4bb6feea2689f3bf215c34e85882c7082f052df311ed50ea8039e330c7bbadbce1061e774e95c5ef90f3b3b45f7bddce820dc248162fe1a7ba9f4a82c7d5801a5d936ca6f0eafe427cbf91dff696aec723205b69338e930f0fc01504b0708c9717be2f502000047050000504b0304140008080800d4ad282b00000000000000000000000007000000432e636c617373458ebb0ac2401045efc61825be059b74763ec074368aa04141b13260bfe81256e2aec48df85b5682851fe04789a3820ecceb72e7308fe7ed0ea08f1a030b72600cde8e9fb81f7315f9ab5419b917d3f3461c8cd42a870c436d3c99ff94dedb4ca7827291c53708e20ca59266c4d0682dffbcd024524583f6ba08170517368a0ca540aba3e1caac799c12c70ef4969a1bea34d988998c059ab0c84b607870405fa24c9b853c4dec8da25a21a54e9a45dde974af285dbecfa0faf1665f504b0708cf75737cbc000000eb000000504b01021400140008080800d4ad282b2bc2bf352a000000280000001400040000000000000000000000000000004d4554412d494e462f4d414e49464553542e4d46feca0000504b01021400140008080800d4ad282b71e5b2737c01000009020000070000000000000000000000000070000000412e636c617373504b01021400140008080800d4ad282bc9717be2f502000047050000070000000000000000000000000021020000422e636c617373504b01021400140008080800d4ad282bcf75737cbc000000eb00000007000000000000000000000000004b050000432e636c617373504b05060000000004000400e50000003c0600000000");

        // create a slow / fast block distinction
        // deploy contracts on different VMs for the two chains
        deployOnAVM = deployAVMContract(sender, avmHelloWorldCode);
        System.out.println("AVM tx: " + deployOnAVM.getTransactionHash());
        fastBlock =
                sourceChain.createNewBlockInternal(
                                sourceChain.getBestBlock(),
                                Arrays.asList(deployOnAVM),
                                true,
                                time / 10_000L)
                        .block;

        deployOnFVM = deployFVMContract(sender, avmHelloWorldCode);
        System.out.println("FVM tx: " + deployOnFVM.getTransactionHash());
        slowBlock =
                new AionBlock(
                        sourceChain.createNewBlockInternal(
                                        sourceChain.getBestBlock(),
                                        Arrays.asList(deployOnFVM),
                                        true,
                                        time / 10_000L)
                                .block);

        slowBlock.getHeader().setTimestamp(time / 10_000L + 100);
        time += 100;

        // sourceChain imports only fast block
        connectResult = sourceChain.tryToConnectAndFetchSummary(fastBlock, time, true);
        result = connectResult.getLeft();
        receipt = connectResult.getRight().getReceipts().get(0);

        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.isSuccessful()).isTrue();

        AionAddress contract = receipt.getTransaction().getContractAddress();

        // ensuring that the same code was given to the FVM transaction
        assertThat(sourceChain.getRepository().getCode(contract))
                .isEqualTo(avmTransformedHelloWorldCode);
        System.out.println(
                "Code Hash: "
                        + Hex.toHexString(
                                HashUtil.h256(sourceChain.getRepository().getCode(contract))));

        // testChain imports both blocks
        connectResult = testChain.tryToConnectAndFetchSummary(fastBlock, time, true);
        result = connectResult.getLeft();
        receipt = connectResult.getRight().getReceipts().get(0);

        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.isSuccessful()).isTrue();
        assertThat(receipt.getTransaction().getContractAddress()).isEqualTo(contract);

        connectResult = testChain.tryToConnectAndFetchSummary(slowBlock, time, true);
        result = connectResult.getLeft();
        receipt = connectResult.getRight().getReceipts().get(0);

        assertThat(result).isEqualTo(ImportResult.IMPORTED_NOT_BEST);
        assertThat(receipt.isSuccessful()).isTrue();
        assertThat(receipt.getTransaction().getContractAddress()).isEqualTo(contract);

        // ****** check that the correct contract details are kept ******

        // TODO
        //        // check that main chain is correct
        //
        // assertThat(sourceChain.getRepository().getVMUsed(contract)).isEqualTo(InternalVmType.AVM);
        //
        // assertThat(testChain.getRepository().getVMUsed(contract)).isEqualTo(InternalVmType.AVM);

        // check information details
        // the fast check database returns the correct VM type for each contract instance
        byte[] codeHashAVM = sourceChain.getRepository().getAccountState(contract).getCodeHash();
        testChain.getRepository().setRoot(slowBlock.getStateRoot());
        byte[] codeHashFVM = testChain.getRepository().getAccountState(contract).getCodeHash();
        testChain.getRepository().setRoot(slowBlock.getStateRoot());

        ContractInformation infoSingleImport =
                sourceChain.getRepository().getIndexedContractInformation(contract);
        System.out.println("without side chain:" + infoSingleImport);

        assertThat(infoSingleImport.getVmUsed(codeHashAVM)).isEqualTo(InternalVmType.AVM);
        assertThat(infoSingleImport.getInceptionBlocks(codeHashAVM))
                .isEqualTo(Set.of(fastBlock.getHashWrapper()));
        assertThat(infoSingleImport.getVmUsed(codeHashFVM)).isEqualTo(InternalVmType.UNKNOWN);
        assertThat(infoSingleImport.getInceptionBlocks(codeHashFVM)).isEmpty();

        ContractInformation infoMultiImport =
                testChain.getRepository().getIndexedContractInformation(contract);
        System.out.println("with side chain:" + infoMultiImport);

        assertThat(infoMultiImport.getVmUsed(codeHashAVM)).isEqualTo(InternalVmType.AVM);
        assertThat(infoMultiImport.getInceptionBlocks(codeHashAVM))
                .isEqualTo(Set.of(fastBlock.getHashWrapper()));
        assertThat(infoMultiImport.getVmUsed(codeHashFVM)).isEqualTo(InternalVmType.FVM);
        assertThat(infoMultiImport.getInceptionBlocks(codeHashFVM))
                .isEqualTo(Set.of(slowBlock.getHashWrapper()));
    }

    /**
     * Ensures that if a side-chain block is imported after a main-chain block creating the same
     * contract address X but using different VMs, then each chain will operate on the correct VM.
     */
    @Test
    public void testVmTypeRetrieval_ImportSideChainWithConflictingContractVM() {
        // blocks to be built
        AionBlock block, fastBlock, slowBlock, highBlock, lowBlock;

        // transactions used in blocks
        AionTransaction deployOnAVM, deployOnFVM, callAVM, callFVM;
        List<AionTransaction> txs;

        // for processing block results
        Pair<ImportResult, AionBlockSummary> connectResult;
        ImportResult result;
        AionTxReceipt receipt;

        // build a blockchain
        TransactionTypeRule.allowAVMContractTransaction();
        List<ECKey> accounts = generateAccounts(10);
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain sourceChain =
                builder.withValidatorConfiguration("simple")
                        .withDefaultAccounts(accounts)
                        .build()
                        .bc;
        StandaloneBlockchain testChain =
                builder.withValidatorConfiguration("simple")
                        .withDefaultAccounts(accounts)
                        .build()
                        .bc;
        ECKey sender = accounts.remove(0);

        assertThat(testChain).isNotEqualTo(sourceChain);
        assertThat(testChain.genesis).isEqualTo(sourceChain.genesis);

        long time = System.currentTimeMillis();

        // ****** setup side chain ******

        // create a slow / fast block distinction
        // deploy contracts on different VMs for the two chains
        deployOnAVM = deployHelloWorldAVM(sender);
        fastBlock =
                sourceChain.createNewBlockInternal(
                                sourceChain.getBestBlock(),
                                Arrays.asList(deployOnAVM),
                                true,
                                time / 10_000L)
                        .block;

        deployOnFVM = deployContract(sender);
        slowBlock =
                new AionBlock(
                        sourceChain.createNewBlockInternal(
                                        sourceChain.getBestBlock(),
                                        Arrays.asList(deployOnFVM),
                                        true,
                                        time / 10_000L)
                                .block);

        slowBlock.getHeader().setTimestamp(time / 10_000L + 100);
        time += 100;

        // sourceChain imports only fast block
        connectResult = sourceChain.tryToConnectAndFetchSummary(fastBlock, time, true);
        result = connectResult.getLeft();
        receipt = connectResult.getRight().getReceipts().get(0);

        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.isSuccessful()).isTrue();

        AionAddress contract = receipt.getTransaction().getContractAddress();

        // testChain imports both blocks
        connectResult = testChain.tryToConnectAndFetchSummary(fastBlock, time, true);
        result = connectResult.getLeft();
        receipt = connectResult.getRight().getReceipts().get(0);

        assertThat(result).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.isSuccessful()).isTrue();
        assertThat(receipt.getTransaction().getContractAddress()).isEqualTo(contract);

        connectResult = testChain.tryToConnectAndFetchSummary(slowBlock, time, true);
        result = connectResult.getLeft();
        receipt = connectResult.getRight().getReceipts().get(0);

        assertThat(result).isEqualTo(ImportResult.IMPORTED_NOT_BEST);
        assertThat(receipt.isSuccessful()).isTrue();
        assertThat(receipt.getTransaction().getContractAddress()).isEqualTo(contract);

        // ****** check that the correct contract details are kept ******

        // TODO: get account state and code hash
        //        // check that main chain is correct
        //
        // assertThat(sourceChain.getRepository().getVMUsed(contract)).isEqualTo(InternalVmType.AVM);
        //
        // assertThat(testChain.getRepository().getVMUsed(contract)).isEqualTo(InternalVmType.AVM);

        // check information details
        // the fast check database returns the correct VM type for each contract instance
        byte[] codeHashAVM = sourceChain.getRepository().getAccountState(contract).getCodeHash();
        testChain.getRepository().setRoot(slowBlock.getStateRoot());
        byte[] codeHashFVM = testChain.getRepository().getAccountState(contract).getCodeHash();
        testChain.getRepository().setRoot(slowBlock.getStateRoot());

        ContractInformation infoSingleImport =
                sourceChain.getRepository().getIndexedContractInformation(contract);
        System.out.println("without side chain:" + infoSingleImport);

        assertThat(infoSingleImport.getVmUsed(codeHashAVM)).isEqualTo(InternalVmType.AVM);
        assertThat(infoSingleImport.getInceptionBlocks(codeHashAVM))
                .isEqualTo(Set.of(fastBlock.getHashWrapper()));
        assertThat(infoSingleImport.getVmUsed(codeHashFVM)).isEqualTo(InternalVmType.UNKNOWN);
        assertThat(infoSingleImport.getInceptionBlocks(codeHashFVM)).isEmpty();

        ContractInformation infoMultiImport =
                testChain.getRepository().getIndexedContractInformation(contract);
        System.out.println("with side chain:" + infoMultiImport);

        assertThat(infoMultiImport.getVmUsed(codeHashAVM)).isEqualTo(InternalVmType.AVM);
        assertThat(infoMultiImport.getInceptionBlocks(codeHashAVM))
                .isEqualTo(Set.of(fastBlock.getHashWrapper()));
        assertThat(infoMultiImport.getVmUsed(codeHashFVM)).isEqualTo(InternalVmType.FVM);
        assertThat(infoMultiImport.getInceptionBlocks(codeHashFVM))
                .isEqualTo(Set.of(slowBlock.getHashWrapper()));
    }

    private AionTransaction deployHelloWorldAVM(ECKey sender) {
        byte[] helloAVM =
                new CodeAndArguments(
                                JarBuilder.buildJarForMainAndClassesAndUserlib(AvmHelloWorld.class),
                                new byte[0])
                        .encodeToBytes();

        AionTransaction transaction =
                new AionTransaction(
                        BigInteger.ZERO.toByteArray(),
                        new AionAddress(sender.getAddress()),
                        null,
                        BigInteger.ZERO.toByteArray(),
                        helloAVM,
                        5_000_000L,
                        10_123_456_789L,
                        TransactionTypes.AVM_CREATE_CODE);

        transaction.sign(sender);

        return transaction;
    }

    private AionTransaction deployFVMContract(ECKey sender, byte[] code) {
        AionTransaction contractDeploymentTx =
                new AionTransaction(
                        BigInteger.ZERO.toByteArray(),
                        null,
                        BigInteger.ZERO.toByteArray(),
                        code,
                        5_000_000L,
                        10_123_456_789L);

        contractDeploymentTx.sign(sender);

        return contractDeploymentTx;
    }

    private AionTransaction deployAVMContract(ECKey sender, byte[] code) {
        AionTransaction transaction =
                new AionTransaction(
                        BigInteger.ZERO.toByteArray(),
                        new AionAddress(sender.getAddress()),
                        null,
                        BigInteger.ZERO.toByteArray(),
                        code,
                        5_000_000L,
                        10_123_456_789L,
                        TransactionTypes.AVM_CREATE_CODE);

        transaction.sign(sender);

        return transaction;
    }

    /*
     * Tests VM update behaviour from an external perspective
     */
}
