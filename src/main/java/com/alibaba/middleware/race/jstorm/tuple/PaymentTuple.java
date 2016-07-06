package com.alibaba.middleware.race.jstorm.tuple;

import com.alibaba.middleware.race.model.PaymentMessage;

import java.io.Serializable;

/**
 * Created by Huiyi on 2016/7/4.
 */
public class PaymentTuple implements Serializable {
    private long orderId;
    private long payAmount;
    private short paySource;
    private short payPlatform;
    private long createTime;

    //无参构造函数，以备kryto 序列化
    public PaymentTuple(){

    }
    public PaymentTuple(PaymentMessage msg) {
        this.orderId = msg.getOrderId();
        this.payAmount = (long)(msg.getPayAmount() * 100);
        this.paySource = msg.getPaySource();
        this.payPlatform = msg.getPayPlatform();
        this.createTime = msg.getCreateTime() / 60000 * 60;
    }

    public PaymentTuple(long orderId, long payAmount, short paySource,
                        short payPlatform, long createTime){
        this.orderId = orderId;
        this.payAmount = payAmount;
        this.paySource = paySource;
        this.payPlatform = payPlatform;
        this.createTime = createTime;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public long getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(long payAmount) {
        this.payAmount = payAmount;
    }

    public short getPaySource() {
        return paySource;
    }

    public void setPaySource(short paySource) {
        this.paySource = paySource;
    }

    public short getPayPlatform() {
        return payPlatform;
    }

    public void setPayPlatform(short payPlatform) {
        this.payPlatform = payPlatform;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public int hashCode() {
        //int 10位: createTime 10 位,1467xxx, paySource 0-3, payAmount < 100
        int shortOrderId = (int)(0X3FFFFFFFL | orderId); //trim 30 bit, i.e 9 digit
        long hashCode = shortOrderId | payAmount | (paySource << 8) | (payPlatform << 9) | createTime;
        return (int)hashCode;
    }
    @Override
    public String toString() {
	return "paymentTuple: { id: "+orderId+" ￥"+payAmount+", paySource: "+paySource+", payPlatform: "
		+payPlatform+" , createMinute: "+createTime+" }";
  }
}
