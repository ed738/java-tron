package org.tron.core.capsule;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Hash;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.store.MarketOrderStore;
import org.tron.protos.Protocol.MarketAccountOrder;
import org.tron.protos.Protocol.MarketAccountOrder.OrderLinkedList;

@Slf4j(topic = "capsule")
public class MarketAccountOrderCapsule implements ProtoCapsule<MarketAccountOrder> {

  private MarketAccountOrder accountOrder;

  public MarketAccountOrderCapsule(final MarketAccountOrder accountOrder) {
    this.accountOrder = accountOrder;
  }

  public MarketAccountOrderCapsule(final byte[] data) {
    try {
      this.accountOrder = MarketAccountOrder.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
    }
  }

  public MarketAccountOrderCapsule(ByteString address) {
    OrderLinkedList orderLinkedList = OrderLinkedList.newBuilder()
        .setHead(ByteString.copyFrom(new byte[0]))
        .setTail(ByteString.copyFrom(new byte[0]))
        .build();

    this.accountOrder = MarketAccountOrder.newBuilder()
        .setOwnerAddress(address)
        .setOrderLinkedList(orderLinkedList)
        .build();
  }

  public ByteString getOwnerAddress() {
    return this.accountOrder.getOwnerAddress();
  }

  public byte[] getOrderHead() {
    return this.accountOrder.getOrderLinkedList().getHead().toByteArray();
  }

  public byte[] getOrderTail() {
    return this.accountOrder.getOrderLinkedList().getTail().toByteArray();
  }

  public boolean isOrderListEmpty() {
    return this.accountOrder.getOrderLinkedList().getHead().isEmpty();
  }

  public void setOrderHead(byte[] head) {
    this.accountOrder = this.accountOrder.toBuilder()
        .setOrderLinkedList(
            this.accountOrder.getOrderLinkedList().toBuilder().setHead(ByteString.copyFrom(head))
                .build()).build();
  }

  public void setOrderTail(byte[] tail) {
    this.accountOrder = this.accountOrder.toBuilder()
        .setOrderLinkedList(
            this.accountOrder.getOrderLinkedList().toBuilder().setTail(ByteString.copyFrom(tail))
                .build()).build();
  }

  public void addOrderToLinkedList(MarketOrderCapsule currentCapsule, MarketOrderStore orderStore)
    throws ItemNotFoundException {
    byte[] orderId = currentCapsule.getID().toByteArray();

    if (this.isOrderListEmpty()) {
      this.setOrderHead(orderId);
      this.setOrderTail(orderId);
    } else {
      // tail.next = order
      // order.pre = tail
      // this.tail = order
      byte[] tailId = this.getOrderTail();
      MarketOrderCapsule tailCapsule = orderStore.get(tailId);
      tailCapsule.setAccountNext(orderId);
      orderStore.put(tailId, tailCapsule);

      currentCapsule.setAccountPrev(tailId);
      orderStore.put(orderId, currentCapsule);

      this.setOrderTail(orderId);
    }

  }

  // just for test
  public MarketOrderCapsule getOrderByIndex(int index, MarketOrderStore marketOrderStore)
      throws ItemNotFoundException {
    if (this.isOrderListEmpty()) {
      return null;
    }

    MarketOrderCapsule current = marketOrderStore.get(this.getOrderHead());

    int count = 0;
    while (current != null) {
      if (count == index) {
        return current;
      }
      count++;

      if (current.isAccountNextNull()) {
        return null;
      }
      current = marketOrderStore.get(current.getAccountNext());
    }

    return null;
  }

  // just for test
  public int getOrderSize(MarketOrderStore marketOrderStore) throws ItemNotFoundException {
    if (this.isOrderListEmpty()) {
      return 0;
    }

    MarketOrderCapsule head = marketOrderStore.get(this.getOrderHead());

    int size = 1;
    while (!head.isAccountNextNull()) {
      size++;
      head = marketOrderStore.get(head.getAccountNext());
    }

    return size;
  }

  // just for test
  public List<ByteString> getOrderIdList(MarketOrderStore marketOrderStore)
      throws ItemNotFoundException {
    List<ByteString> orderIdList = new ArrayList<>();

    if (this.isOrderListEmpty()) {
      return orderIdList;
    }

    MarketOrderCapsule current = marketOrderStore.get(this.getOrderHead());

    while (current != null) {
      orderIdList.add(current.getID());

      if (current.isAccountNextNull()) {
        return orderIdList;
      }
      current = marketOrderStore.get(current.getAccountNext());
    }

    return orderIdList;
  }

  public void setCount(long o) {
    this.accountOrder = this.accountOrder.toBuilder()
        .setCount(o)
        .build();
  }

  public long getCount() {
    return this.accountOrder.getCount();
  }

  public void setOwnerAddress(long count) {
    this.accountOrder = this.accountOrder.toBuilder()
        .setCount(count)
        .build();
  }


  @Override
  public byte[] getData() {
    return this.accountOrder.toByteArray();
  }

  @Override
  public MarketAccountOrder getInstance() {
    return this.accountOrder;
  }

}
