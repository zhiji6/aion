package org.aion.zero.impl.db;

import java.math.BigInteger;
import org.aion.rlp.RLP;
import org.aion.rlp.RLPList;

/**
 * Indexed information about contracts that is not part of consensus. Used to
 *
 * <ol>
 *   <li>quickly find the block where the contract was created;
 *   <li>determine which virtual machine was used in deploying the contract;
 *   <li>check if the contract information is complete during fast sync.
 * </ol>
 *
 * @author Alexandra Roatis
 */
public class ContractInformation {
    private long inceptionBlock;
    private byte vmUsed;
    private boolean complete;

    public long getInceptionBlock() {
        return inceptionBlock;
    }

    public int getVmUsed() {
        return vmUsed;
    }

    public boolean isComplete() {
        return complete;
    }

    /**
     * Decodes a {@link ContractInformation} object from the RLP encoding.
     *
     * @param rlpEncoded The encoding to decode.
     */
    public static ContractInformation decode(byte[] rlpEncoded) {
        if (rlpEncoded == null || rlpEncoded.length == 0) {
            return null;
        } else {
            RLPList list = (RLPList) RLP.decode2(rlpEncoded).get(0);
            if (list.size() != 3) {
                return null;
            } else {
                // create and populate object
                ContractInformation info = new ContractInformation();

                // decode the inception block
                info.inceptionBlock = new BigInteger(1, list.get(0).getRLPData()).longValue();
                // TODO: some sort of validation

                // decode the VM used
                byte[] array = list.get(1).getRLPData();
                if (array.length != 1) {
                    return null;
                } else {
                    info.vmUsed = array[0];
                }

                // decode the completeness status
                array = list.get(2).getRLPData();
                if (array.length != 1) {
                    return null;
                } else {
                    info.complete = array[0] == 1;
                }

                return info;
            }
        }
    }

    /**
     * Returns an RLP encoding of this ContractInformation object.
     *
     * @return an RLP encoding of this ContractInformation object.
     */
    public byte[] getEncoded() {
        // NOTE: not using encodeLong because of the non-standard RLP
        byte[] rlpBlockNumber = RLP.encode(inceptionBlock);
        byte[] rlpVmUsed = RLP.encodeByte(vmUsed);
        byte[] rlpIsComplete = RLP.encodeByte((byte) (complete ? 1 : 0));

        return RLP.encodeList(rlpBlockNumber, rlpVmUsed, rlpIsComplete);
    }
}
