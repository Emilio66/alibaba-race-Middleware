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

    public PaymentTuple(PaymentMessage msg) {
        this.orderId = msg.getOrderId();
        this.payAmount = (long)(msg.getPayAmount() * 100);
        this.paySource = msg.getPaySource();
        this.payPlatform = msg.getPayPlatform();
        this.createTime = msg.getCreateTime();
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
}
