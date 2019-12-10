package org.aion.api.server.external.account;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.aion.crypto.ECKey;
import org.aion.crypto.ECKeyFac;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.log.LogLevel;
import org.aion.zero.impl.keystore.Keystore;
import org.aion.types.AionAddress;
import org.aion.util.types.AddressUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AccountManagerTest {
    private static AccountManager accountManager = new AccountManager(null);
    private AionAddress notRegistered =
            AddressUtils.wrapAddress(
                    "a011111111111111111111111111111101010101010101010101010101010101");
    private final int DEFAULT_TEST_TIMEOUT = 10;

    private static ECKey k1;
    private static ECKey k2;
    private static ECKey k3;

    private static final String p1 = "password1";
    private static final String p2 = "password2";
    private static final String p3 = "password3";

    private static String address1;
    private static String address2;
    private static String address3;

    private static  String KEYSTORE_PATH;

    @BeforeClass
    public static void setupClass() {
        AionLoggerFactory.initAll(Map.of(LogEnum.API, LogLevel.DEBUG));

        KEYSTORE_PATH = Keystore.getKeystorePath();
        k1 = ECKeyFac.inst().create();
        k2 = ECKeyFac.inst().create();
        k3 = ECKeyFac.inst().create();

        // record the addresses to be used later when removing files from the system
        address1 = Keystore.create(p1, k1).substring(2);
        address2 = Keystore.create(p2, k2).substring(2);
        address3 = Keystore.create(p3, k3).substring(2);
    }

    @AfterClass
    public static void cleanClass() {
        accountManager.removeAllAccounts();
        // remove the files created
        cleanFiles();
    }

    @After
    public void cleanManager() {
        // empty the map in account manager for each test
        cleanAccountManager();
    }

    @Test
    public void testUnlockAccount() {
        // unlock 2 accounts
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));
        long timeOutTotal1 = Instant.now().getEpochSecond() + DEFAULT_TEST_TIMEOUT;
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k2.getAddress()), p2, DEFAULT_TEST_TIMEOUT));
        long timeOutTotal2 = Instant.now().getEpochSecond() + DEFAULT_TEST_TIMEOUT;

        // check account manager
        List<Account> list = accountManager.getAccounts();

        int returnedTimeout1 = (int) list.get(0).getTimeout();
        int returnedTimeout2 = (int) list.get(1).getTimeout();
        byte[] returnedAddress1 = list.get(0).getKey().getAddress();
        byte[] returnedAddress2 = list.get(1).getKey().getAddress();

        // since the returned list is not ordered, have to check for all possible orders
        assertTrue(
                Arrays.equals(returnedAddress1, k1.getAddress())
                        || Arrays.equals(returnedAddress1, k2.getAddress()));
        assertTrue(
                Arrays.equals(returnedAddress2, k1.getAddress())
                        || Arrays.equals(returnedAddress2, k2.getAddress()));

        // same with the timeout, since there could be a slight(1s) difference between each unlock
        // as well
        assertTrue(returnedTimeout1 == timeOutTotal1 || returnedTimeout1 == timeOutTotal2);
        assertTrue(returnedTimeout2 == timeOutTotal1 || returnedTimeout2 == timeOutTotal2);
    }

    @Test
    public void testUnlockAccountUpdateTimeout() {
        // update the timeout from 1s to 2s
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));
        assertTrue(accountManager.unlockAccount(new AionAddress(k1.getAddress()), p1, 20));

        // check that the timeout is updated
        assertEquals(accountManager.getAccounts().get(0).getTimeout(), Instant.now().getEpochSecond() + 20);
        assertEquals(1, accountManager.getAccounts().size());
    }

    @Test
    public void testUnlockAccountWithNotRegisteredKey() {
        assertFalse(
                accountManager.unlockAccount(notRegistered, "no password", DEFAULT_TEST_TIMEOUT));

        // check that no account has been put into the manager
        assertEquals(0, accountManager.getAccounts().size());
    }

    @Test
    public void testUnlockAccountWithWrongPassword() {
        assertFalse(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), "not p1", DEFAULT_TEST_TIMEOUT));

        // check that no account has been put into the manager
        assertEquals(0, accountManager.getAccounts().size());
    }

    @Test
    public void testUnlockAccountTimeoutGreaterThanMax() {
        // unlock account with timeout greater than max
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, AccountManager.UNLOCK_MAX + 10));

        // check that the recoded timeout is no bigger than max
        assertEquals(accountManager.getAccounts().get(0).getTimeout(), Instant.now().getEpochSecond() + AccountManager.UNLOCK_MAX);

        // now update the timeout back to a small value so it can be cleared easily during @After
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));
    }

    @Test
    public void testUnlockAccountWithNegativeTimeout() {
        // try to unlock account with a negative integer as the timeout
        assertTrue(accountManager.unlockAccount(new AionAddress(k1.getAddress()), p1, -1));
        int expectedTimeout = (int) Instant.now().getEpochSecond() + AccountManager.UNLOCK_DEFAULT;

        // check that the account is created and added to the manager
        assertEquals(1, accountManager.getAccounts().size());
        assertEquals(k1.toString(), accountManager.getAccounts().get(0).getKey().toString());

        // however the timeout is changed to the default timeout in account manager
        assertEquals(expectedTimeout, accountManager.getAccounts().get(0).getTimeout());
    }

    @Test
    public void testLockAccount() {
        // first unlock an account
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));

        // now try to lock it, the timeout will change
        assertTrue(accountManager.lockAccount(new AionAddress(k1.getAddress()), p1));

        // check that the account is now locked
        List<Account> accountList = accountManager.getAccounts();
        assertEquals(1, accountList.size());
        assertTrue(accountList.get(0).getTimeout() < Instant.now().getEpochSecond());
        assertArrayEquals(accountList.get(0).getKey().getAddress(), k1.getAddress());
    }

    @Test
    public void testLockAccountNotInManager() {
        // first unlock an account
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));

        // try to lock a different account
        assertTrue(accountManager.lockAccount(new AionAddress(k2.getAddress()), p2));

        // check that there is still only the first account in the manager
        assertEquals(1, accountManager.getAccounts().size());
    }

    @Test
    public void testLockAccountWithNotRegisteredKey() {
        assertFalse(accountManager.lockAccount(notRegistered, "no password"));

        // check that no account has been put into the manager
        assertEquals(0, accountManager.getAccounts().size());
    }

    @Test
    public void testLockAccountWithWrongPassword() {
        // first unlock an account
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT + 1));

        // check if its there
        assertEquals(1, accountManager.getAccounts().size());

        // try to lock with wrong password
        assertFalse(accountManager.lockAccount(new AionAddress(k1.getAddress()), "not p1"));
    }

    @Test
    public void testGetKeyReturned() {
        // first unlock an account
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));

        // retrieve the key
        ECKey ret = accountManager.getKey(new AionAddress(k1.getAddress()));

        // check equality
        assertArrayEquals(ret.getAddress(), k1.getAddress());
    }

    @Test
    public void testGetKeyRemoved() {
        // first unlock an account
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, DEFAULT_TEST_TIMEOUT));

        // lock the account
        assertTrue(accountManager.lockAccount(new AionAddress(k1.getAddress()), p1));

        // retrieve key, but instead it is removed
        assertNull(accountManager.getKey(new AionAddress(k1.getAddress())));

        // check that it was removed
        assertEquals(0, accountManager.getAccounts().size());
    }

    @Test
    public void testGetKeyNotInMap() {
        // check that there are currently no accounts in the manager
        assertEquals(0, accountManager.getAccounts().size());

        // try to get a key not in the manager
        assertNull(accountManager.getKey(new AionAddress(k1.getAddress())));
    }

    @Test
    public void testUnlockAndLockMultipleTimes() {
        // first an account
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, AccountManager.UNLOCK_DEFAULT));
        assertEquals(1, accountManager.getAccounts().size());

        // lock k1 and check that timeout is changed
        assertTrue(accountManager.lockAccount(new AionAddress(k1.getAddress()), p1));
        List<Account> accountsList;
        accountsList = accountManager.getAccounts();
        assertEquals(1, accountsList.size());
        assertTrue(accountsList.get(0).getTimeout() < Instant.now().getEpochSecond());

        // now unlock account with k1 again and check that timeout is changed
        assertTrue(
                accountManager.unlockAccount(
                        new AionAddress(k1.getAddress()), p1, AccountManager.UNLOCK_DEFAULT));
        assertEquals(1, accountManager.getAccounts().size());
        assertEquals(accountsList.get(0).getTimeout(), Instant.now().getEpochSecond() + AccountManager.UNLOCK_DEFAULT);
    }

    private static void cleanAccountManager() {
        // lock all the accounts, which modifies the timeout
        accountManager.lockAccount(new AionAddress(k1.getAddress()), p1);
        accountManager.lockAccount(new AionAddress(k2.getAddress()), p2);
        accountManager.lockAccount(new AionAddress(k3.getAddress()), p3);

        // remove accounts
        accountManager.getKey(new AionAddress(k1.getAddress()));
        accountManager.getKey(new AionAddress(k2.getAddress()));
        accountManager.getKey(new AionAddress(k3.getAddress()));

        // check that manager is cleared
        assertEquals(0, accountManager.getAccounts().size());
    }

    private static void cleanFiles() {
        // get a list of all the files in keystore directory
        File folder = new File(KEYSTORE_PATH);
        File[] AllFilesInDirectory = folder.listFiles();
        List<String> allFileNames = new ArrayList<>();
        List<String> filesToBeDeleted = new ArrayList<>();

        // check for invalid or wrong path - should not happen
        if (AllFilesInDirectory == null) return;

        for (File file : AllFilesInDirectory) {
            allFileNames.add(file.getName());
        }

        // get a list of the files needed to be deleted, check the ending of file names with
        // corresponding addresses
        for (String name : allFileNames) {
            String ending = "";
            if (name.length() > 64) {
                ending = name.substring(name.length() - 64);
            }

            if (ending.equals(address1) || ending.equals(address2) || ending.equals(address3)) {
                filesToBeDeleted.add(KEYSTORE_PATH + "/" + name);
            }
        }

        // iterate and delete those files
        for (String name : filesToBeDeleted) {
            File file = new File(name);
            file.delete();
        }
    }
}
