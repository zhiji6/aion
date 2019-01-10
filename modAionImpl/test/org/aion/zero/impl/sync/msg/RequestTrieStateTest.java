package org.aion.zero.impl.sync.msg;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.zero.impl.sync.TrieDatabase.DETAILS;
import static org.aion.zero.impl.sync.TrieDatabase.STATE;
import static org.aion.zero.impl.sync.TrieDatabase.STORAGE;

import java.util.ArrayList;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.aion.rlp.RLP;
import org.aion.zero.impl.sync.TrieDatabase;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Unit tests for {@link RequestTrieState} messages.
 *
 * @author Alexandra Roatis
 */
@RunWith(JUnitParamsRunner.class)
public class RequestTrieStateTest {
    private static final byte[] nodeKey =
            new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31, 32
            };
    private static final byte[] altNodeKey =
            new byte[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31
            };
    private static final byte[] zeroNodeKey = new byte[32];
    private static final byte[] smallNodeKey =
            new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31
            };
    private static final byte[] largeNodeKey =
            new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31, 32, 33
            };
    private static final byte[] emptyByteArray = new byte[] {};

    /**
     * Parameters for testing:
     *
     * <ul>
     *   <li>{@link #testDecode_correct(byte[], TrieDatabase, int)}
     *   <li>{@link #testEncode_correct(byte[], TrieDatabase, int)}
     *   <li>{@link #testEncodeDecode(byte[], TrieDatabase, int)}
     * </ul>
     */
    @SuppressWarnings("unused")
    private Object correctParameters() {
        List<Object> parameters = new ArrayList<>();

        byte[][] keyOptions = new byte[][] {nodeKey, altNodeKey, zeroNodeKey};
        TrieDatabase[] dbOptions = new TrieDatabase[] {STATE, STORAGE, DETAILS};
        int[] limitOptions = new int[] {0, 1, 10, Integer.MAX_VALUE};

        // network and directory
        String[] net_values = new String[] {"mainnet", "invalid"};
        for (byte[] key : keyOptions) {
            for (TrieDatabase db : dbOptions) {
                for (int limit : limitOptions) {
                    parameters.add(new Object[] {key, db, limit});
                }
            }
        }

        return parameters.toArray();
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullKey() {
        new RequestTrieState(null, STATE, 10);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullType() {
        new RequestTrieState(nodeKey, null, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_negativeLimit() {
        new RequestTrieState(nodeKey, STATE, -10);
    }

    @Test
    public void testDecode_nullMessage() {
        assertThat(RequestTrieState.decode(null)).isNull();
    }

    @Test
    public void testDecode_emptyMessage() {
        assertThat(RequestTrieState.decode(emptyByteArray)).isNull();
    }

    @Test
    public void testDecode_missingType() {
        byte[] encoding = RLP.encodeList(RLP.encodeElement(nodeKey), RLP.encodeInt(0));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_missingKey() {
        byte[] encoding = RLP.encodeList(RLP.encodeString(STATE.toString()), RLP.encodeInt(0));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_missingLimit() {
        byte[] encoding =
                RLP.encodeList(RLP.encodeElement(nodeKey), RLP.encodeString(STATE.toString()));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_additionalValue() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(nodeKey),
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeInt(0),
                        RLP.encodeInt(10));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_outOfOrder() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeElement(nodeKey),
                        RLP.encodeInt(0));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_incorrectType() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(nodeKey), RLP.encodeString("random"), RLP.encodeInt(10));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_smallerKeySize() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(smallNodeKey),
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeInt(10));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    public void testDecode_largerKeySize() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(largeNodeKey),
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeInt(10));
        assertThat(RequestTrieState.decode(encoding)).isNull();
    }

    @Test
    @Parameters(method = "correctParameters")
    public void testDecode_correct(byte[] key, TrieDatabase dbType, int limit) {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(key),
                        RLP.encodeString(dbType.toString()),
                        RLP.encodeInt(limit));

        RequestTrieState message = RequestTrieState.decode(encoding);

        assertThat(message).isNotNull();
        assertThat(message.getNodeKey()).isEqualTo(key);
        assertThat(message.getDbType()).isEqualTo(dbType);
        assertThat(message.getLimit()).isEqualTo(limit);
    }

    @Test
    @Parameters(method = "correctParameters")
    public void testEncode_correct(byte[] key, TrieDatabase dbType, int limit) {
        byte[] expected =
                RLP.encodeList(
                        RLP.encodeElement(key),
                        RLP.encodeString(dbType.toString()),
                        RLP.encodeInt(limit));

        RequestTrieState message = new RequestTrieState(key, dbType, limit);
        assertThat(message.encode()).isEqualTo(expected);
    }

    @Test
    @Parameters(method = "correctParameters")
    public void testEncodeDecode(byte[] key, TrieDatabase dbType, int limit) {
        // encode
        RequestTrieState message = new RequestTrieState(key, dbType, limit);

        assertThat(message.getNodeKey()).isEqualTo(key);
        assertThat(message.getDbType()).isEqualTo(dbType);
        assertThat(message.getLimit()).isEqualTo(limit);

        byte[] encoding = message.encode();

        // decode
        RequestTrieState decoded = RequestTrieState.decode(encoding);

        assertThat(decoded).isNotNull();

        assertThat(decoded.getNodeKey()).isEqualTo(key);
        assertThat(decoded.getDbType()).isEqualTo(dbType);
        assertThat(decoded.getLimit()).isEqualTo(limit);
    }

    @Test
    public void testEncode_differentKey() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(nodeKey),
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeInt(Integer.MAX_VALUE));

        RequestTrieState message = new RequestTrieState(altNodeKey, STATE, Integer.MAX_VALUE);
        assertThat(message.encode()).isNotEqualTo(encoding);
    }

    @Test
    public void testEncode_differentType() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(nodeKey),
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeInt(Integer.MAX_VALUE));

        RequestTrieState message = new RequestTrieState(nodeKey, STORAGE, Integer.MAX_VALUE);
        assertThat(message.encode()).isNotEqualTo(encoding);
    }

    @Test
    public void testEncode_differentLimit() {
        byte[] encoding =
                RLP.encodeList(
                        RLP.encodeElement(nodeKey),
                        RLP.encodeString(STATE.toString()),
                        RLP.encodeInt(Integer.MAX_VALUE));

        RequestTrieState message = new RequestTrieState(nodeKey, STATE, 0);
        assertThat(message.encode()).isNotEqualTo(encoding);
    }
}
