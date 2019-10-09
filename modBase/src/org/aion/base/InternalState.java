package org.aion.mcf.core;

/**
 * Functionality for recording the state of a account or contract.
 *
 * @author Alexandra Roatis
 */
public class InternalState {

    /** The RLP encoding of this account state. */
    private byte[] rlpEncoding = null;

    /** Flag indicating whether the account has been deleted. */
    private boolean deleted = false;
    /** Flag indicating whether the state of the account has been changed. */
    private boolean dirty = false;

    public InternalState() {}

    public InternalState(boolean dirty, boolean deleted) {
        this.dirty = dirty;
        this.deleted = deleted;
    }

    public InternalState(boolean dirty, boolean deleted, byte[] rlpEncoding) {
        this(dirty, deleted);
        this.rlpEncoding = rlpEncoding;
    }

    /**
     * Checks whether the state of the account has been changed during execution.
     *
     * <p>The dirty status is set internally by the object when its stored values have been
     * modified.
     *
     * @return {@code true} if the account state has been modified (including if the account is
     *     newly created), {@code false} if the account state is the same as in the database
     */
    public boolean isDirty() {
        return this.dirty;
    }

    /**
     * Sets the dirty flag to true signaling that the object state has been changed.
     *
     * @apiNote Once the account has been modified (by setting the flag to {@code true}) it is
     *     <b>considered dirty</b> even if its state reverts to the initial values by applying
     *     subsequent changes.
     * @implNote Method called internally by the account object when its state has been modified.
     *     Resets the stored RLP encoding.
     */
    public void markDirty() {
        this.rlpEncoding = null;
        this.dirty = true;
    }

    /**
     * Checks if the state of the account has been deleted.
     *
     * @return {@code true} if the account was deleted, {@code false} otherwise
     */
    public boolean isDeleted() {
        return this.deleted;
    }

    /** Marks the state as deleted and dirty. */
    public void markDeleted() {
        this.deleted = true;
        markDirty();
    }

    /**
     * Retrieves the RLP encoding of this object.
     *
     * @return a {@code byte} array representing the RLP encoding of the account state.
     * @implNote For performance reasons, this encoding is stored when available and recomputed only
     *     if the object has been modified during execution.
     */
    public byte[] getEncoding() {
        return rlpEncoding;
    }

    /**
     * Stores the RLP encoding of the tracked object.
     *
     * @param rlpEncoding a {@code byte} array representing the RLP encoding of the object state
     */
    public void setEncoding(byte[] rlpEncoding) {
        this.rlpEncoding = rlpEncoding;
    }
}
