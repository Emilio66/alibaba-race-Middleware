package com.alibaba.middleware.race.jstorm.tuple;

import backtype.storm.tuple.TupleExt;
import com.alibaba.middleware.race.model.OrderMessage;

import java.io.Serializable;

/**
 * 我们后台RocketMq存储的订单消息模型类似于OrderMessage，选手也可以自定义
 * 订单消息模型，只要模型中各个字段的类型和顺序和OrderMessage一样，即可用Kryo
 * 反序列出消息
 */
public class OrderTuple implements Serializable {
    private static final long serialVersionUID = -4082657304129211564L;
    private long orderId; //订单ID
    private short orderType; // 0 - taobao, 1 - tmall

    public OrderTuple() {

    }

    public OrderTuple(long orderId, short orderType) {
        this.orderId = orderId;
        this.orderType = orderType;
    }

    @Override
    public String toString() {
        return "OrderMessage{ orderId=" + getOrderId() + ", orderType=" + getOrderType() + "}";
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public short getOrderType() {
        return orderType;
    }

    public void setOrderType(short orderType) {
        this.orderType = orderType;
    }
}
