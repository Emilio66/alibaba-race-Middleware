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
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by Huiyi on 2016/7/8.
 */
public class HashBolt implements IRichBolt {

    private static final Logger LOG = Logger.getLogger(HashBolt.class);

    protected OutputCollector collector;
    protected Set<Long> taobaoOrder = new HashSet<>();
    protected Set<Long> tmallOrder = new HashSet<>();
    protected LinkedBlockingDeque<PaymentTuple> paymentBuffer = new LinkedBlockingDeque<>();

    class BufferThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    PaymentTuple payment = paymentBuffer.take();
                    if (taobaoOrder.contains(payment.getOrderId())) {
                        collector.emit(RaceConfig.TAOBAO_DISPATCH_STREAM, new Values(payment));
                    } else if (tmallOrder.contains(payment.getOrderId())) {
                        collector.emit(RaceConfig.TMALL_DISPATCH_STREAM, new Values(payment));
                    } else {
                        paymentBuffer.put(payment);
                        Thread.sleep(10);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        BufferThread buffer = new HashBolt.BufferThread();
        buffer.start();
    }

    @Override
    public void execute(Tuple tuple) {
        long orderId = tuple.getLong(0);
        PaymentTuple payment = (PaymentTuple) tuple.getValue(1);
        OrderTuple order = (OrderTuple) tuple.getValue(2);

        LOG.info("HashBolt get [ Order ID: " + orderId + " ]");
        if (payment != null) {
            LOG.info(payment.toString());
            try {
                paymentBuffer.put(payment);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (order != null) {
            LOG.info(order.toString());
            if (order.getOrderType() == 0 && !taobaoOrder.contains(orderId)) {
                taobaoOrder.add(orderId);
            }
            if (order.getOrderType() == 1 && !tmallOrder.contains(orderId)) {
                tmallOrder.add(orderId);
            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.TAOBAO_DISPATCH_STREAM, new Fields("payment"));
        declarer.declareStream(RaceConfig.TMALL_DISPATCH_STREAM, new Fields("payment"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
