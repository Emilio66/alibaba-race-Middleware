package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.tuple.OrderTuple;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by zhaoz on 2016/7/10.
 * payment list来了split 一下，扔出去的是payment
 * 集成上一个版本CountBolt功能
 * count bolt 中做消息去重和累加，直接save
 */
public class SplitBolt implements IRichBolt{
    public static final Logger LOG = Logger.getLogger(SplitBolt.class);
    private OutputCollector collector;

    private Set<Long> taobaoOrder = new HashSet<>();
    private Set<Long> tmallOrder = new HashSet<>();
    private Map<Long, MsgObject> msgMap = new HashMap<>();

    private LinkedBlockingDeque<PaymentTuple> paymentBuffer = new LinkedBlockingDeque<>();

    class BufferThread extends Thread {

        @Override
        public void run() {
            try {
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            while (true) {
                try {
                    PaymentTuple payment = paymentBuffer.take();

                    if (taobaoOrder.contains(payment.getOrderId())) {
                        collector.emit(RaceConfig.taobaoStream, new Values(payment));
                    } else if (tmallOrder.contains(payment.getOrderId())) {
                        collector.emit(RaceConfig.tmallStream, new Values(payment));
                    } else {
                        paymentBuffer.addFirst(payment);
                        Thread.sleep(10);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//        BufferThread buffer = new SplitBolt.BufferThread();
//        buffer.start();
    }

    @Override
    public void execute(Tuple input) {
        Object field1 = input.getValue(0);
        Object field2 = input.getValue(1);

        if (field1 != null) {
            List<PaymentTuple> paymentTuples = (List<PaymentTuple>) field1;
            for (PaymentTuple payment : paymentTuples) {
                //LOG.info("Get payment: " + payment);
//                paymentBuffer.addLast(payment);
                MsgObject msgObj = msgMap.get(payment.getOrderId());
                if (msgObj == null) {
                    MsgObject obj = new MsgObject();
                    obj.getPaymentTuples().add(payment);
                    msgMap.put(payment.getOrderId(), obj);
                } else if (msgObj.getOrderTuple() == null) {
                    msgObj.getPaymentTuples().add(payment);
                } else {
                    collector.emit(getStreamNameByType(msgObj.getOrderTuple().getOrderType()), new Values(payment));
                }

            }
        } else {
            List<OrderTuple> orderTuples = (List<OrderTuple>) field2;
            for (OrderTuple order : orderTuples) {
                //LOG.info("Get order: " + order.toString());
                /*if (order.getOrderType() == 0) { // taobao order
                    taobaoOrder.add(order.getOrderId());
                } else {
                    tmallOrder.add(order.getOrderId());
                }*/
                MsgObject msgObj = msgMap.get(order.getOrderId());
                if (msgObj == null) {
                    MsgObject obj = new MsgObject();
                    obj.setOrderTuple(order);
                    msgMap.put(order.getOrderId(), obj);
                } else if (msgObj.getOrderTuple() == null) {
                    msgObj.setOrderTuple(order);
                    List<PaymentTuple> existingPayments = msgObj.getPaymentTuples();
                    for (PaymentTuple payment : existingPayments) {
                        collector.emit(getStreamNameByType(order.getOrderType()), new Values(payment));
                    }
                    msgObj.getPaymentTuples().clear();

                } else {
                    // should not enter this branch.
                }
            }
        }
    }

    private String getStreamNameByType(short orderType) {
        return orderType == 0 ? RaceConfig.taobaoStream : RaceConfig.tmallStream;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.taobaoStream, new Fields("payment"));
        declarer.declareStream(RaceConfig.tmallStream, new Fields("payment"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    class MsgObject {
        private List<PaymentTuple> paymentTuples = new ArrayList<>();
        private OrderTuple orderTuple;


        public List<PaymentTuple> getPaymentTuples() {
            return paymentTuples;
        }

        public void setPaymentTuples(List<PaymentTuple> paymentTuples) {
            this.paymentTuples = paymentTuples;
        }

        public OrderTuple getOrderTuple() {
            return orderTuple;
        }

        public void setOrderTuple(OrderTuple orderTuple) {
            this.orderTuple = orderTuple;
        }
    }
}
