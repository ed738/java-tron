package stest.tron.wallet.market;

import com.beust.jcommander.internal.Lists;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.tron.api.GrpcAPI.EmptyMessage;
import org.tron.api.WalletGrpc;
import org.tron.common.crypto.ECKey;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Utils;
import org.tron.core.Wallet;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.TransactionInfo;
import stest.tron.wallet.common.client.Configuration;
import stest.tron.wallet.common.client.Parameter.CommonConstant;
import stest.tron.wallet.common.client.utils.PublicMethed;

@Slf4j
public class WalletTestMarket {

  private final String testKey006 = Configuration.getByPath("testng.conf")
      .getString("witness.key6");
  private final byte[] fromAddress = PublicMethed.getFinalAddress(testKey006);

  ArrayList<String> txidList = new ArrayList<String>();
  Optional<TransactionInfo> infoById = null;
  Long beforeTime;
  Long afterTime;
  Long beforeBlockNum;
  Long afterBlockNum;
  Block currentBlock;
  Long currentBlockNum;
  private ManagedChannel channelFull = null;
  private WalletGrpc.WalletBlockingStub blockingStubFull = null;
  private String fullnode = Configuration.getByPath("testng.conf")
      .getStringList("fullnode.ip.list").get(0);


  List<String> accountAddressList = Lists.newArrayList();
  List<String> accountKeyList = Lists.newArrayList();

  int newAccountNum = 100;
  int newOrderNum = 20;

  @BeforeSuite
  public void beforeSuite() {
    logger.info("beforeSuite");
    Wallet wallet = new Wallet();
    Wallet.setAddressPreFixByte(CommonConstant.ADD_PRE_FIX_BYTE_MAINNET);
  }

  /**
   * constructor.
   */

  @BeforeClass(enabled = true)
  public void beforeClass() {
    logger.info("beforeClass");
    PublicMethed.printAddress(testKey006);
//    PublicMethed.printAddress(testKey003);
    channelFull = ManagedChannelBuilder.forTarget(fullnode)
        .usePlaintext(true)
        .build();
    blockingStubFull = WalletGrpc.newBlockingStub(channelFull);
    currentBlock = blockingStubFull.getNowBlock(EmptyMessage.newBuilder().build());
    beforeBlockNum = currentBlock.getBlockHeader().getRawData().getNumber();
    beforeTime = System.currentTimeMillis();

    createAccount();
    sendCoin();
    sendToken();
    createMarketOrderWithoutMatch();
    logger.info("wait 6000 for precondition");//
    try {
      Thread.sleep(6000);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private void createAccount() {
    String ownerKey = testKey006;
    byte[] ownerAddress = fromAddress;

    int index = 0;

    while (index++ < newAccountNum) {
      ECKey ecKey1 = new ECKey(Utils.getRandom());
      byte[] accountAddress = ecKey1.getAddress();
      String testKeyAccount = ByteArray.toHexString(ecKey1.getPrivKeyBytes());
      accountAddressList.add(ByteArray.toHexString(accountAddress));
      accountKeyList.add(testKeyAccount);

      boolean ret = PublicMethed
          .createAccount(ownerAddress, accountAddress, ownerKey, blockingStubFull);
      logger.info("createAccount");

    }
  }

  private void sendCoin() {
    String ownerKey = testKey006;
    byte[] ownerAddress = fromAddress;
    for (String address : accountAddressList) {
      byte[] accountAddress = ByteArray.fromHexString(address);
      PublicMethed
          .sendcoin(accountAddress, 10_000_000L, ownerAddress, ownerKey, blockingStubFull);
      logger.info("sendcoin");
    }

  }

  private void sendToken() {
    String ownerKey = testKey006;
    byte[] ownerAddress = fromAddress;
    for (String address : accountAddressList) {
      byte[] accountAddress = ByteArray.fromHexString(address);
      PublicMethed
          .transferAsset(accountAddress, "1000001".getBytes(),100_000_000L, ownerAddress, ownerKey, blockingStubFull);
      logger.info("sendToken");
    }

  }

//  @Test(enabled = true, threadPoolSize = 1, invocationCount = 1)
  public void createMarketOrderWithoutMatch() {
    byte[] sellTokenID = "_".getBytes();
    byte[] buyTokenID = "1000001".getBytes();
    long sellTokenQuantity = 2000;
    createMarketOrder(sellTokenID, buyTokenID, sellTokenQuantity, 3000, 2000,
        "create sell order");
//
  }


    @Test(enabled = true, threadPoolSize = 1, invocationCount = 1)
  public void testCreateMarketOrderWithMatch() throws Exception {


    byte[] sellTokenID = "1000001".getBytes();
    byte[] buyTokenID = "_".getBytes();
    long sellTokenQuantity = 3000;
    createMarketOrder(sellTokenID, buyTokenID, sellTokenQuantity, 2000, 1000,
        "create buy order");
  }

  public void createMarketOrder(byte[] sellTokenID, byte[] buyTokenID
      , long sellTokenQuantity, int buyTokenQuantityMax, int buyTokenQuantityMin,String message) {


    Integer j = 0;

    long start = System.currentTimeMillis();
    while (j++ < newOrderNum) {

      IntStream.range(0, accountAddressList.size()).parallel().forEach(i -> {
        int randomNum = Utils.getRandom().nextInt((buyTokenQuantityMax - buyTokenQuantityMin) + 1)
            + buyTokenQuantityMin;
        long buyTokenQuantity = randomNum;
        if (buyTokenQuantity < 0) {
          buyTokenQuantity = -buyTokenQuantity;
        }

        String address = accountAddressList.get(i);
        String key = accountKeyList.get(i);

        byte[] accountAddress = ByteArray.fromHexString(address);
        boolean ret = false;
        try {
          ret = PublicMethed
              .sellMarketOrder(sellTokenID, buyTokenID, sellTokenQuantity, buyTokenQuantity,
                  accountAddress, key, blockingStubFull);
         } catch (Exception ex) {
          logger.error("", ex);
        }

        logger.info(message);
      });
      long interval = System.currentTimeMillis() - start;
      logger.info("interval:" + interval);
      if (interval < 3000) {
        try {
          Thread.sleep(3000 - interval);
        } catch (Exception ex) {
          ex.printStackTrace();
        }

      }
      start = System.currentTimeMillis();
    }
  }

  /**
   * constructor.
   */

  @AfterClass
  public void shutdown() throws InterruptedException {

    if (channelFull != null) {
      channelFull.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}