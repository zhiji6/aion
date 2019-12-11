package org.aion.zero.impl.db;

import static org.aion.crypto.HashUtil.EMPTY_DATA_HASH;

import java.util.Optional;
import org.aion.base.ConstantUtil;
import org.aion.mcf.db.ContractDetails;
import org.aion.mcf.db.InternalVmType;
import org.aion.rlp.RLP;
import org.aion.rlp.RLPElement;
import org.aion.rlp.RLPItem;
import org.aion.rlp.RLPList;
import org.aion.types.AionAddress;
import org.aion.zero.impl.trie.SecureTrie;

/**
 * Class used to construct {@link org.aion.mcf.db.ContractDetails} instances.
 *
 * @author Alexandra Roatis
 */
public class ContractDetailsFactory {
    public static AionContractDetailsImpl fromEncoding(byte[] encoding) {
        if (encoding == null) {
            throw new NullPointerException("Cannot decode ContractDetails from null RLP encoding.");
        }
        if (encoding.length == 0) {
            throw new IllegalArgumentException("Cannot decode ContractDetails from empty RLP encoding.");
        }

        decode(code);
    }

    /**
     * Decodes an AionContractDetailsImpl object from the RLP encoding rlpCode.
     *
     * @param rlpCode The encoding to decode.
     * @implNote IMPORTANT: Requires the VM type to be set externally before decoding. The way the
     *     data is interpreted during decoding differs for AVM contracts, therefore it will be
     *     decoded incorrectly if the VM type is not set before making this method call.
     */
    public void decode(byte[] rlpCode) {
        // TODO: remove vm type requirement when refactoring into separate AVM & FVM implementations
        decode(rlpCode, false);
    }

    /**
     * Decodes an AionContractDetailsImpl object from the RLP encoding rlpCode. The fast check flag
     * indicates whether the contractDetails needs to sync with external storage.
     *
     * @param rlpCode The encoding to decode.
     * @param fastCheck indicates whether the contractDetails needs to sync with external storage.
     */
    public void decode(byte[] rlpCode, boolean fastCheck) {
        RLPList data = RLP.decode2(rlpCode);

        RLPList rlpList = (RLPList) data.get(0);

        // partial decode either encoding
        boolean keepStorageInMem = decodeEncodingWithoutVmType(rlpList, fastCheck);

        if (rlpList.size() != 5) {
            // revert back from storing the VM type in details
            // force a save with new encoding
            throw new IllegalStateException("Incompatible data storage. Please shutdown the kernel and perform database migration to version 1.0 (Denali) of the kernel as instructed in the release.");
        } else {
            // keep encoding when compatible with new style
            this.rlpEncoded = rlpCode;
        }

        if (!fastCheck || externalStorage || !keepStorageInMem) { // it was not a fast check
            // NOTE: under normal circumstances the VM type is set by the details data store
            // Do not forget to set the vmType value externally during tests!!!
            decodeStorage(rlpList.get(2), rlpList.get(3), keepStorageInMem);
        }
    }

    /**
     * Decodes part of the old version of encoding which was a list of 5 elements, specifically:<br>
     * { 0:address, 1:isExternalStorage, 2:storageRoot, 3:storage, 4:code } <br>
     * without processing the storage information.
     *
     * <p>The 2:storageRoot and 3:storage must be processed externally to apply the distinct
     * interpretations based on the type of virtual machine.
     *
     * @return {@code true} if the storage must continue to be kept in memory, {@code false}
     *     otherwise
     */
    public boolean decodeEncodingWithoutVmType(RLPList rlpList, boolean fastCheck) {
        RLPItem isExternalStorage = (RLPItem) rlpList.get(1);
        RLPItem storage = (RLPItem) rlpList.get(3);
        this.externalStorage = isExternalStorage.getRLPData().length > 0;
        boolean keepStorageInMem = storage.getRLPData().length <= detailsInMemoryStorageLimit;

        // No externalStorage require.
        if (fastCheck && !externalStorage && keepStorageInMem) {
            return keepStorageInMem;
        }

        RLPItem address = (RLPItem) rlpList.get(0);
        RLPElement code = rlpList.get(4);

        if (address == null
                || address.getRLPData() == null
                || address.getRLPData().length != AionAddress.LENGTH) {
            throw new IllegalArgumentException("rlp decode error: invalid contract address");
        } else {
            this.address = new AionAddress(address.getRLPData());
        }

        if (code instanceof RLPList) {
            for (RLPElement e : ((RLPList) code)) {
                setCode(e.getRLPData());
            }
        } else {
            setCode(code.getRLPData());
        }
        return keepStorageInMem;
    }

    /** Instantiates the storage interpreting the storage root according to the VM specification. */
    public void decodeStorage(RLPElement root, RLPElement storage, boolean keepStorageInMem) {
        // different values based on the VM used
        byte[] storageRootHash;
        if (vmType == InternalVmType.AVM) {
            // points to the storage hash and the object graph hash
            concatenatedStorageHash = root.getRLPData();

            Optional<byte[]> concatenatedData =
                    objectGraphSource == null
                            ? Optional.empty()
                            : getContractObjectGraphSource().get(concatenatedStorageHash);
            if (concatenatedData.isPresent()) {
                RLPList data = RLP.decode2(concatenatedData.get());
                if (!(data.get(0) instanceof RLPList)) {
                    throw new IllegalArgumentException(
                            "rlp decode error: invalid concatenated storage for AVM");
                }
                RLPList pair = (RLPList) data.get(0);
                if (pair.size() != 2) {
                    throw new IllegalArgumentException(
                            "rlp decode error: invalid concatenated storage for AVM");
                }

                storageRootHash = pair.get(0).getRLPData();
                objectGraphHash = pair.get(1).getRLPData();
            } else {
                storageRootHash = ConstantUtil.EMPTY_TRIE_HASH;
                objectGraphHash = EMPTY_DATA_HASH;
            }
        } else {
            storageRootHash = root.getRLPData();
        }

        // load/deserialize storage trie
        if (externalStorage) {
            storageTrie = new SecureTrie(getExternalStorageDataSource(), storageRootHash);
        } else {
            storageTrie.deserialize(storage.getRLPData());
        }
        storageTrie.withPruningEnabled(prune > 0);

        // switch from in-memory to external storage
        if (!externalStorage && !keepStorageInMem) {
            externalStorage = true;
            storageTrie.getCache().setDB(getExternalStorageDataSource());
        }
    }

}
