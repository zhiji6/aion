package org.aion.zero.impl.db;

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
    private int vmUsed;
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
}
