package org.aion.api.server.rpc3;

import java.math.BigInteger;
import org.aion.mcf.blockchain.Block;
import org.aion.types.AionAddress;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionTxInfo;
import org.aion.zero.impl.types.BlockContext;
import org.aion.zero.impl.types.StakingBlock;

/*
 * All access to the Aion Blockchain should be handled by this class.
 * This allows easier testing as interactions within this class can easily be mocked.
 */
public interface ChainHolder {

    /**
     * Returns a block based on the specified block number.
     * In order to determine the type of block returned callers should check the seal type field
     * @param block The block number to be queried
     * @return
     */
    Block getBlockByNumber(long block);

    /**
     * @return the block at the head of the block chain
     */
    Block getBestBlock();

    /**
     * Returns the block based on the specified hash
     * @param hash The block hash to be queried
     * @return
     */
    Block getBlockByHash(byte[] hash);

    /**
     * @param hash a block hash
     * @return the total difficulty at the specified block hash
     */
    BigInteger getTotalDifficultyByHash(byte[] hash);

    /**
     *
     * @param transactionHash the hash of the transaction to be queried
     * @return the aionTxInfo of the specified transaction
     */
    AionTxInfo getTransactionInfo(byte[] transactionHash);

    /**
     * Calculates the block reward at the block number.
     * @param number a block number
     * @return the block reward
     */
    BigInteger calculateReward(Long number);

    /**
     * Checks whether the unity fork has been enabled
     * @return
     */
    boolean isUnityForkEnabled();

    /**
     * This should be preceded by a call to can seal
     * Attempts to seal a block with the given signature
     * @param signature the signature of the validator
     * @param sealHash the hash of the block to be sealed
     * @return true if the block was sealed
     */
    boolean submitSignature(byte[] signature, byte[] sealHash);

    /**
     *
     * @param newSeed
     * @param signingPublicKey the public key of the pub-priv key pair that will be used to sign the block
     * @param coinBase the account which will receive the block reward
     * @return the seal hash of the block
     */
    byte[] submitSeed(byte[] newSeed, byte[] signingPublicKey, byte[] coinBase);

    /**
     * @return a seed that can be used to sign a pos block
     */
    byte[] getSeed();

    /**
     *
     * @return a block template
     */
    BlockContext getBlockTemplate();

    /**
     * This should be proceeded by a call to can seal.
     * @param nonce the nonce of the block
     * @param solution the equihash solution
     * @param headerHash the hash to be used to retrieve the block template
     * @return true if the block was sealed
     */
    boolean submitBlock(byte[] nonce, byte[] solution, byte[] headerHash);

    /**
     * @return the last pow block sealed
     */
    AionBlock getBestPOWBlock();

    /**
     * @return the last pos block sealed
     */
    StakingBlock getBestPOSBlock();

    /**
     * @param address the address to be queried
     * @return true if the address exists in the keystore
     */
    boolean addressExists(AionAddress address);

    /**
     * @param headerHash the hash to be used to retrieve the block
     * @return true if the blockchain is currently caching this block template
     */
    boolean canSeal(byte[] headerHash);

    /**
     * Attempts to import and propagate the new block on the network
     * @param block the proposed block
     * @return true if the block was imported best or not best
     */
    boolean addNewBlock(Block block);
}
