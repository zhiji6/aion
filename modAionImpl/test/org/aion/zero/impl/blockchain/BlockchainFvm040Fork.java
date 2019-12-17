package org.aion.zero.impl.blockchain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Collections;
import org.aion.base.AionTransaction;
import org.aion.base.TransactionTypes;
import org.aion.base.TxUtil;
import org.aion.crypto.ECKey;
import org.aion.fastvm.FastVmResultCode;
import org.aion.zero.impl.core.ImportResult;
import org.aion.types.AionAddress;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionBlockSummary;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class BlockchainFvm040Fork {

    private String deepContractCode =
            "6050604052600a604051805910620000145750595b9080825280601002601001820160405280156200002c5"
                    + "75b506000600050908051906010019062000047929190620000aa565b50600a601260005090905560"
                    + "2060405190810160405280600481526010016f61696f6e00000000000000000000000081526010015"
                    + "0602560005090805190601001906200009692919062000103565b503415620000a45760006000fd5b"
                    + "620001c0565b8280548282559060005260106000209050908101928215620000f0579160100282015"
                    + "b82811115620000ef5782518260005090905591601001919060010190620000cd565b5b5090506200"
                    + "00ff919062000191565b5090565b82805460018160011615610100020316600290049060005260106"
                    + "00020905090600f016010900481019282600f106200014857805160ff19168380011785556200017e"
                    + "565b828001600101855582156200017e579182015b828111156200017d57825182600050909055916"
                    + "010019190600101906200015b565b5b5090506200018d919062000191565b5090565b620001bd9190"
                    + "6200019d565b80821115620001b957600081815060009055506001016200019d565b5090565b90565"
                    + "b61126080620001d06000396000f3006050604052361561007f576000356c01000000000000000000"
                    + "000000900463ffffffff168062b9c66514610085578063016eabe0146100bd5780634df7e3d014610"
                    + "44857806367a09e7e146104e25780637d67d3b31461054e57806381bff170146105645780639e0ca1"
                    + "651461057a578063f220ff7b146106a05761007f565b60006000fd5b34156100915760006000fd5b6"
                    + "100a760048080359060100190919050506107bd565b60405180828152601001915050604051809103"
                    + "90f35b34156100c95760006000fd5b6100d16107ef565b60405180806010018060100180601001806"
                    + "01001806010018060100180601001806010018981038952b181815181526010019150805190601001"
                    + "9080838360005b8381101561012e5780820151818401525b601081019050610112565b50505050905"
                    + "090810190600f16801561015b5780820380516001836010036101000a031916815260100191505b50"
                    + "8981038852b0818151815260100191508051906010019080838360005b83811015610195578082015"
                    + "1818401525b601081019050610179565b50505050905090810190600f1680156101c2578082038051"
                    + "6001836010036101000a031916815260100191505b5089810387528f8181518152601001915080519"
                    + "06010019080838360005b838110156101fc5780820151818401525b6010810190506101e0565b5050"
                    + "5050905090810190600f1680156102295780820380516001836010036101000a03191681526010019"
                    + "1505b5089810386528e818151815260100191508051906010019080838360005b8381101561026357"
                    + "80820151818401525b601081019050610247565b50505050905090810190600f16801561029057808"
                    + "20380516001836010036101000a031916815260100191505b5089810385528d818151815260100191"
                    + "508051906010019080838360005b838110156102ca5780820151818401525b6010810190506102ae5"
                    + "65b50505050905090810190600f1680156102f75780820380516001836010036101000a0319168152"
                    + "60100191505b5089810384528c818151815260100191508051906010019080838360005b838110156"
                    + "103315780820151818401525b601081019050610315565b50505050905090810190600f1680156103"
                    + "5e5780820380516001836010036101000a031916815260100191505b5089810383528b81815181526"
                    + "0100191508051906010019080838360005b838110156103985780820151818401525b601081019050"
                    + "61037c565b50505050905090810190600f1680156103c55780820380516001836010036101000a031"
                    + "916815260100191505b5089810382528a818151815260100191508051906010019080838360005b83"
                    + "8110156103ff5780820151818401525b6010810190506103e3565b50505050905090810190600f168"
                    + "01561042c5780820380516001836010036101000a031916815260100191505b50c050505050505050"
                    + "5050505050505050505060405180910390f35b34156104545760006000fd5b61045c610d66565b604"
                    + "05180b28152601001b18152601001b081526010018f81526010018e81526010018d81526010018c81"
                    + "526010018b81526010018a81526010018981526010018881526010018781526010018681526010018"
                    + "58152601001848152601001838152601001828152601001c150505050505050505050505050505050"
                    + "505060405180910390f35b34156104ee5760006000fd5b6104f6610e08565b6040518080601001828"
                    + "103825283818151815260100191508051906010019060100280838360005b8381101561053a578082"
                    + "0151818401525b60108101905061051e565b505050509050019250505060405180910390f35b34156"
                    + "1055a5760006000fd5b610562610f1c565b005b34156105705760006000fd5b610578610ff3565b00"
                    + "5b34156105865760006000fd5b61068a6004808080601001359035909160200190919290808060100"
                    + "135903590916020019091929080806010013590359091602001909192908080601001359035909160"
                    + "200190919290808060100135903590916020019091929080806010013590359091602001909192908"
                    + "080601001359035909160200190919290808060100135903590916020019091929080806010013590"
                    + "359091602001909192908080601001359035909160200190919290808060100135903590916020019"
                    + "091929080806010013590359091602001909192908080601001359035909160200190919290808060"
                    + "10013590359091602001909192908035906010019091905050611073565b604051808281526010019"
                    + "1505060405180910390f35b34156106ac5760006000fd5b6107a76004808080601001359035909160"
                    + "200190919290808060100135903590916020019091929080806010013590359091602001909192908"
                    + "080601001359035909160200190919290808060100135903590916020019091929080806010013590"
                    + "359091602001909192908080601001359035909160200190919290808060100135903590916020019"
                    + "091929080806010013590359091602001909192908080601001359035909160200190919290808060"
                    + "100135903590916020019091929080806010013590359091602001909192908080601001359035909"
                    + "1602001909192908080601001359035909160200190919290505061111c565b604051808281526010"
                    + "0191505060405180910390f35b6000600082146107e0576107d9600183036107bd63ffffffff16565"
                    + "b82016107e3565b60005b90506107ea565b919050565b6107f761115f565b6107ff61115f565b6108"
                    + "0761115f565b61080f61115f565b61081761115f565b61081f61115f565b61082761115f565b61082"
                    + "f61115f565b6025600050602560005060256000506025600050602560005060256000506025600050"
                    + "6025600050878054600181600116156101000203166002900480600f0160108091040260100160405"
                    + "190810160405280929190818152601001828054600181600116156101000203166002900480156108"
                    + "ee5780600f106108c1576101008083540402835291601001916108ee565b820191906000526010600"
                    + "0209050905b8154815290600101906010018083116108d157829003600f168201915b505050505097"
                    + "50868054600181600116156101000203166002900480600f016010809104026010016040519081016"
                    + "04052809291908181526010018280546001816001161561010002031660029004801561098c578060"
                    + "0f1061095f5761010080835404028352916010019161098c565b82019190600052601060002090509"
                    + "05b81548152906001019060100180831161096f57829003600f168201915b50505050509650858054"
                    + "600181600116156101000203166002900480600f01601080910402601001604051908101604052809"
                    + "2919081815260100182805460018160011615610100020316600290048015610a2a5780600f106109"
                    + "fd57610100808354040283529160100191610a2a565b8201919060005260106000209050905b81548"
                    + "1529060010190601001808311610a0d57829003600f168201915b5050505050955084805460018160"
                    + "0116156101000203166002900480600f0160108091040260100160405190810160405280929190818"
                    + "15260100182805460018160011615610100020316600290048015610ac85780600f10610a9b576101"
                    + "00808354040283529160100191610ac8565b8201919060005260106000209050905b8154815290600"
                    + "10190601001808311610aab57829003600f168201915b505050505094508380546001816001161561"
                    + "01000203166002900480600f016010809104026010016040519081016040528092919081815260100"
                    + "182805460018160011615610100020316600290048015610b665780600f10610b3957610100808354"
                    + "040283529160100191610b66565b8201919060005260106000209050905b815481529060010190601"
                    + "001808311610b4957829003600f168201915b50505050509350828054600181600116156101000203"
                    + "166002900480600f01601080910402601001604051908101604052809291908181526010018280546"
                    + "0018160011615610100020316600290048015610c045780600f10610bd75761010080835404028352"
                    + "9160100191610c04565b8201919060005260106000209050905b81548152906001019060100180831"
                    + "1610be757829003600f168201915b5050505050925081805460018160011615610100020316600290"
                    + "0480600f0160108091040260100160405190810160405280929190818152601001828054600181600"
                    + "11615610100020316600290048015610ca25780600f10610c75576101008083540402835291601001"
                    + "91610ca2565b8201919060005260106000209050905b815481529060010190601001808311610c855"
                    + "7829003600f168201915b50505050509150808054600181600116156101000203166002900480600f"
                    + "016010809104026010016040519081016040528092919081815260100182805460018160011615610"
                    + "100020316600290048015610d405780600f10610d1357610100808354040283529160100191610d40"
                    + "565b8201919060005260106000209050905b815481529060010190601001808311610d23578290036"
                    + "00f168201915b5050505050905097509750975097509750975097509750610d5c565b909192939495"
                    + "9697565b6001600050806000016000505490806001016000505490806002016000505490806003016"
                    + "000505490806004016000505490806005016000505490806006016000505490806007016000505490"
                    + "80600801600050549080600901600050549080600a01600050549080600b01600050549080600c016"
                    + "00050549080600d01600050549080600e01600050549080600f016000505490806010016000505490"
                    + "50b1565b610e10611176565b610e1861118d565b60a06040519081016040528060018152601001600"
                    + "281526010016003815260100160048152601001600581526010016006815260100160078152601001"
                    + "6008815260100160098152601001600a8152601001509050606360006000506006815481101515610"
                    + "e8257fe5b906000526010600020905090600191828204019190066010025b50819090905550806000"
                    + "60005090600a610eb79291906111b5565b50600060005080548060100260100160405190810160405"
                    + "2809291908181526010018280548015610f0c576010028201919060005260106000209050905b8160"
                    + "00505481526010019060010190808311610ef5575b50505050509150610f18565b5090565b600a600"
                    + "160005060000160005081909090555060136000600082016000506000905560018201600050600090"
                    + "556002820160005060009055600382016000506000905560048201600050600090556005820160005"
                    + "060009055600682016000506000905560078201600050600090556008820160005060009055600982"
                    + "0160005060009055600a820160005060009055600b820160005060009055600c82016000506000905"
                    + "5600d820160005060009055600e820160005060009055600f82016000506000905560108201600050"
                    + "6000905550505b565b600060006000600060006000600060006000600060006000600060006000600"
                    + "060006000c05060009f5060009e5060009d5060009c5060009b5060009a5060009950600098506000"
                    + "975060009650600095506000945060009350600092506000915060009050600ac050b0505b5050505"
                    + "050505050505050505050505050565b600060018211151561108b578190506110fb566110fa565b7f"
                    + "3c2410eb68658356dd8a61e9386512d25713e98a724d86a9d738b717af7d649c83604051808281526"
                    + "0100191505060405180910390a16110f0bebebebebebebebebebebebebebebebebebebebebebebebe"
                    + "bebebebe6001bf0361107363ffffffff16565b60010190506110fb565b5bcdcc50505050505050505"
                    + "05050505050505050505050505050505050505050565b600060006000600060649250606391508282"
                    + "03905080600003935061113c565b505050cccb5050505050505050505050505050505050505050505"
                    + "0505050505050565b601060405190810160405280600081526010015090565b601060405190810160"
                    + "405280600081526010015090565b60a060405190810160405280600a905b600081526010019060019"
                    + "003908161119d5790505090565b82805482825590600052601060002090509081019282156111f857"
                    + "9160100282015b828111156111f757825182600050909055916010019190600101906111d7565b5b5"
                    + "090506112059190611209565b5090565b6112319190611213565b8082111561122d57600081815060"
                    + "00905550600101611213565b5090565b905600a165627a7a723058204cd044cbfd6e487f90edb6536"
                    + "b2093edf043e7f97676c023f455cde27e41c7050029";
    private byte[] callData =
            Hex.decode(
                    "f220ff7b"
                            + "0000000000000000000000000000000000000000000000000000000000000001"
                            + "0000000000000000000000000000000000000000000000000000000000000002"
                            + "0000000000000000000000000000000000000000000000000000000000000003"
                            + "0000000000000000000000000000000000000000000000000000000000000004"
                            + "0000000000000000000000000000000000000000000000000000000000000005"
                            + "0000000000000000000000000000000000000000000000000000000000000006"
                            + "0000000000000000000000000000000000000000000000000000000000000007"
                            + "0000000000000000000000000000000000000000000000000000000000000008"
                            + "0000000000000000000000000000000000000000000000000000000000000009"
                            + "000000000000000000000000000000000000000000000000000000000000000a"
                            + "000000000000000000000000000000000000000000000000000000000000000b"
                            + "000000000000000000000000000000000000000000000000000000000000000c"
                            + "000000000000000000000000000000000000000000000000000000000000000d"
                            + "000000000000000000000000000000000000000000000000000000000000000e");
    private long energyPrice = 10_000_000_000L;

    @Test
    public void testFVM040hardFork() {
        StandaloneBlockchain.Bundle bundle =
                new StandaloneBlockchain.Builder()
                        .withDefaultAccounts()
                        .withValidatorConfiguration("simple")
                        .build();

        StandaloneBlockchain bc = bundle.bc;
        bc.set040ForkNumber(3L);

        ECKey key = bundle.privateKeys.get(0);
        BigInteger accountNonce = bc.getRepository().getNonce(new AionAddress(key.getAddress()));

        // deploy
        AionTransaction deployTx =
                AionTransaction.create(
                        key,
                        accountNonce.toByteArray(),
                        null,
                        BigInteger.ZERO.toByteArray(),
                        ByteUtil.hexStringToBytes(deepContractCode),
                        1000000L,
                        energyPrice,
                        TransactionTypes.DEFAULT, null);

        AionBlock block1 =
                bc.createNewMiningBlock(bc.getGenesis(), Collections.singletonList(deployTx), true);

        Pair<ImportResult, AionBlockSummary> result = bc.tryToConnectAndFetchSummary(block1);
        assertTrue(result.getLeft().isSuccessful());
        AionTransaction tx = result.getRight().getReceipts().get(0).getTransaction();
        AionAddress contractAddr = TxUtil.calculateContractAddress(tx);
        assertNotNull(contractAddr);

        // excute old fvm logic before fork
        AionTransaction txCall =
                AionTransaction.create(
                        key,
                        accountNonce.add(BigInteger.ONE).toByteArray(),
                        contractAddr,
                        BigInteger.ZERO.toByteArray(),
                        callData,
                        1000000L,
                        energyPrice,
                        TransactionTypes.DEFAULT, null);

        AionBlock block2 = bc.createNewMiningBlock(block1, Collections.singletonList(txCall), true);
        result = bc.tryToConnectAndFetchSummary(block2);
        assertSame(result.getLeft(), ImportResult.IMPORTED_BEST);
        assertEquals(
                FastVmResultCode.OUT_OF_NRG.toString(),
                result.getRight().getSummaries().get(0).getReceipt().getError());

        // excute new fvm logic at fork
        txCall =
                AionTransaction.create(
                        key,
                        accountNonce.add(BigInteger.TWO).toByteArray(),
                        contractAddr,
                        BigInteger.ZERO.toByteArray(),
                        callData,
                        1000000L,
                        energyPrice,
                        TransactionTypes.DEFAULT, null);

        AionBlock block3 = bc.createNewMiningBlock(block2, Collections.singletonList(txCall), true);
        result = bc.tryToConnectAndFetchSummary(block3);

        assertSame(result.getLeft(), ImportResult.IMPORTED_BEST);
        assertEquals("", result.getRight().getSummaries().get(0).getReceipt().getError());
    }
}
