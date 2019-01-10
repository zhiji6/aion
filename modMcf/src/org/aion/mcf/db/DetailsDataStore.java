package org.aion.mcf.db;

import static org.aion.base.util.ByteArrayWrapper.wrap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import org.aion.base.db.IByteArrayKeyValueDatabase;
import org.aion.base.db.IContractDetails;
import org.aion.base.db.IRepositoryConfig;
import org.aion.base.type.Address;
import org.aion.base.type.IBlockHeader;
import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.base.vm.IDataWord;
import org.aion.mcf.trie.JournalPruneDataSource;
import org.aion.mcf.types.AbstractBlock;

/** Detail data storage , */
public class DetailsDataStore<
        BLK extends AbstractBlock<BH, ? extends ITransaction>, BH extends IBlockHeader> {

    private JournalPruneDataSource storageDSPrune;
    private IRepositoryConfig repoConfig;

    private IByteArrayKeyValueDatabase detailsSrc;
    private IByteArrayKeyValueDatabase storageSrc;
    private Set<ByteArrayWrapper> removes = new HashSet<>();

    public DetailsDataStore() {}

    public DetailsDataStore(
            IByteArrayKeyValueDatabase detailsCache,
            IByteArrayKeyValueDatabase storageCache,
            IRepositoryConfig repoConfig) {

        this.repoConfig = repoConfig;
        withDb(detailsCache, storageCache);
    }

    public DetailsDataStore<BLK, BH> withDb(
            IByteArrayKeyValueDatabase detailsSrc, IByteArrayKeyValueDatabase storageSrc) {
        this.detailsSrc = detailsSrc;
        this.storageSrc = storageSrc;
        this.storageDSPrune = new JournalPruneDataSource(storageSrc);
        return this;
    }

    /**
     * Fetches the ContractDetails from the cache, and if it doesn't exist, add to the remove set.
     *
     * @param key
     * @return
     */
    public synchronized IContractDetails<IDataWord> get(byte[] key) {

        ByteArrayWrapper wrappedKey = wrap(key);
        Optional<byte[]> rawDetails = detailsSrc.get(key);

        // If it doesn't exist in cache or database.
        if (!rawDetails.isPresent()) {

            // Check to see if we have to remove it.
            // If it isn't in removes set, we add it to removes set.
            if (!removes.contains(wrappedKey)) {
                removes.add(wrappedKey);
            }
            return null;
        }

        // Found something from cache or database, return it by decoding it.
        IContractDetails<IDataWord> detailsImpl = repoConfig.contractDetailsImpl();
        detailsImpl.setDataSource(storageDSPrune);
        detailsImpl.decode(rawDetails.get()); // We can safely get as we checked
        // if it is present.

        return detailsImpl;
    }

    public synchronized void update(Address key, IContractDetails<IDataWord> contractDetails) {

        contractDetails.setAddress(key);
        ByteArrayWrapper wrappedKey = wrap(key.toBytes());

        // Put into cache.
        byte[] rawDetails = contractDetails == null ? null : contractDetails.getEncoded();
        detailsSrc.put(key.toBytes(), rawDetails);

        contractDetails.syncStorage();

        // Remove from the remove set.
        removes.remove(wrappedKey);
    }

    public synchronized void remove(byte[] key) {
        ByteArrayWrapper wrappedKey = wrap(key);
        detailsSrc.put(key, null);

        removes.add(wrappedKey);
    }

    public synchronized void flush() {
        flushInternal();
    }

    private long flushInternal() {
        long totalSize = 0;

        syncLargeStorage();

        // Get everything from the cache and calculate the size.
        Iterator<byte[]> keysFromSource = detailsSrc.keys();
        while (keysFromSource.hasNext()) {
            byte[] keyInSource = keysFromSource.next();
            // Fetch the value given the keys.
            Optional<byte[]> valFromKey = detailsSrc.get(keyInSource);

            // Add to total size given size of the value
            totalSize += valFromKey.map(rawDetails -> rawDetails.length).orElse(0);
        }

        // Flushes both details and storage.
        detailsSrc.commit();
        storageSrc.commit();

        return totalSize;
    }

    public void syncLargeStorage() {

        Iterator<byte[]> keysFromSource = detailsSrc.keys();
        while (keysFromSource.hasNext()) {
            byte[] keyInSource = keysFromSource.next();

            // Fetch the value given the keys.
            Optional<byte[]> rawDetails = detailsSrc.get(keyInSource);

            // If it is null, just continue
            if (!rawDetails.isPresent()) {
                continue;
            }

            // Decode the details.
            IContractDetails<IDataWord> detailsImpl = repoConfig.contractDetailsImpl();
            detailsImpl.setDataSource(storageDSPrune);
            detailsImpl.decode(rawDetails.get()); // We can safely get as we
            // checked if it is present.

            // IContractDetails details = entry.getValue();
            detailsImpl.syncStorage();
        }
    }

    public JournalPruneDataSource getStorageDSPrune() {
        return storageDSPrune;
    }

    public synchronized Iterator<ByteArrayWrapper> keys() {
        return new DetailsIteratorWrapper(detailsSrc.keys());
    }

    /**
     * A wrapper for the iterator needed by {@link DetailsDataStore} conforming to the {@link
     * Iterator} interface.
     *
     * @author Alexandra Roatis
     */
    private class DetailsIteratorWrapper implements Iterator<ByteArrayWrapper> {
        private Iterator<byte[]> sourceIterator;

        /**
         * @implNote Building two wrappers for the same {@link Iterator} will lead to inconsistent
         *     behavior.
         */
        DetailsIteratorWrapper(final Iterator<byte[]> sourceIterator) {
            this.sourceIterator = sourceIterator;
        }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public ByteArrayWrapper next() {
            return wrap(sourceIterator.next());
        }
    }

    public synchronized void close() {
        try {
            detailsSrc.close();
            storageSrc.close();
        } catch (Exception e) {
            throw new RuntimeException("error closing db");
        }
    }
}
