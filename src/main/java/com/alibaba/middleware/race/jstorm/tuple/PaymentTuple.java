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
        this.createTime = msg.getCreateTime() / 60000;
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
        //1000w订单，10*2^20
        //int 10位: createTime 10 位,1467xxx, paySource 0-3, payAmount < 100
        int shortOrderId = (int)(0XFFFFFFFL & orderId); //trim 28 bit, i.e 9 digit
        int shortTime = (int)(0X0FFFFFFFL & createTime);//10位取7位
        //System.out.println("shortOrderId "+shortOrderId+" paysource left: "+ (paySource << 8)+ " platform left "+ (payPlatform << 9));
        long hashCode = shortOrderId | shortTime | payAmount | (paySource << 10) | (payPlatform << 11) ;
        return (int)hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaymentTuple that = (PaymentTuple) o;

        if (createTime != that.createTime) return false;
        if (orderId != that.orderId) return false;
        if (payAmount != that.payAmount) return false;
        if (payPlatform != that.payPlatform) return false;
        if (paySource != that.paySource) return false;

        return true;
    }

    @Override
    public String toString() {
	return "paymentTuple: { id: "+orderId+" ￥"+payAmount+", paySource: "+paySource+", payPlatform: "
		+payPlatform+" , createMinute: "+createTime+" }";
  }
}
