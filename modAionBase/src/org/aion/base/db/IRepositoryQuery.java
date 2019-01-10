package org.aion.base.db;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.aion.base.type.Address;
import org.aion.base.vm.IDataWord;

/** Repository interface for information retrieval. */
public interface IRepositoryQuery<AS, DW> {

    // getters relating to user accounts
    // -------------------------------------------------------------------------------

    /**
     * Checks if the database contains an account state associated with the given address.
     *
     * @param address the address of the account of interest
     * @return {@code true} if the account exists, {@code false} otherwise
     */
    boolean hasAccountState(Address address);

    /**
     * Loads the account (and contract) state associated with the given address into the given hash
     * maps.
     *
     * @param address the address of the account of interest
     * @param accounts a map representing a cache of {@link AS} where the account state will be
     *     loaded
     * @param details a map representing a cache of {@link IContractDetails<DW>} where the contract
     *     details will be loaded
     */
    void loadAccountState(
            Address address, Map<Address, AS> accounts, Map<Address, IContractDetails<DW>> details);

    /**
     * Retrieves the current state of the account associated with the given address.
     *
     * @param address the address of the account of interest
     * @return a {@link AS} object representing the account state as is stored in the database or
     *     cache
     */
    AS getAccountState(Address address);

    /**
     * Retrieves the current balance of the account associated with the given address.
     *
     * @param address the address of the account of interest
     * @return a {@link BigInteger} value representing the current account balance
     */
    BigInteger getBalance(Address address);

    /**
     * Retrieves the current nonce of the account associated with the given address.
     *
     * @param address the address of the account of interest
     * @return a {@link BigInteger} value representing the current account nonce
     */
    BigInteger getNonce(Address address);

    // getters relating to contracts
    // -----------------------------------------------------------------------------------

    /**
     * Checks if the database contains contract details associated with the given address.
     *
     * @param addr the address of the account of interest
     * @return {@code true} if there are contract details associated with the account, {@code false}
     *     otherwise
     */
    boolean hasContractDetails(Address addr);

    /**
     * Retrieves the contract details of the account associated with the given address.
     *
     * @param addr the address of the account of interest
     * @return a {@link IContractDetails<DW>} object representing the contract details as are stored
     *     in the database or cache
     */
    IContractDetails<DW> getContractDetails(Address addr);

    /**
     * Retrieves the code for the account associated with the given address.
     *
     * @param address the address of the account of interest
     * @return the code associated to the account in {@code byte} array format
     */
    byte[] getCode(Address address);

    // getters relating to storage
    // -------------------------------------------------------------------------------------

    /**
     * Retrieves the entries for the specified key values stored at the account associated with the
     * given address.
     *
     * @param address the address of the account of interest
     * @param keys the collection of keys of interest (which may be {@code null})
     * @return the storage entries for the specified keys, or the full storage if the key collection
     *     is {@code null}
     * @apiNote When called with a null key collection, the method retrieves all the storage keys.
     */
    Map<DW, DW> getStorage(Address address, Collection<DW> keys);

    //    /**
    //     * Retrieves the storage size the account associated with the given address.
    //     *
    //     * @param address
    //     *            the address of the account of interest
    //     * @return the number of storage entries for the given account
    //     */
    //    int getStorageSize(Address address);
    //
    //    /**
    //     * Retrieves all the storage keys for the account associated with the given
    //     * address.
    //     *
    //     * @param address
    //     *            the address of the account of interest
    //     * @return the set of storage keys, or an empty set if the given account
    //     *         address does not exist
    //     */
    //    Set<DW> getStorageKeys(Address address);

    /**
     * Retrieves the stored value for the specified key stored at the account associated with the
     * given address.
     *
     * @param address the address of the account of interest
     * @param key the key of interest
     * @return a {@link DW} representing the data associated with the given key
     */
    IDataWord getStorageValue(Address address, DW key);

    /**
     * Retrieves the stored transactions for recovering pool tx.
     *
     * @return the list of transactions encoded bytes.
     */
    List<byte[]> getPoolTx();

    /**
     * Retrieves the stored transactions for recovering caching tx.
     *
     * @return the list of transactions encoded bytes.
     */
    List<byte[]> getCacheTx();
}
