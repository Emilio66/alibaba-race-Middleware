package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.tuple.OrderTuple;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Huiyi on 2016/7/8.
 */
public class HashSpout implements IRichSpout, MessageListenerConcurrently {

    private static Logger LOG = Logger.getLogger(HashSpout.class);

    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer consumer;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        try {
            consumer = ConsumerFactory.getInstance(JStormUtils.process_pid(), this);
        } catch (Exception e) {
            LOG.error("Failed to create consumer...");
            e.printStackTrace();
        }

        if (consumer == null) {
            LOG.warn("Already exist consumer in current worker, don't need to fetch data");

            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            }).start();
        }
    }

    @Override
    public void nextTuple() {
        // do nothing
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        MessageQueue queue = context.getMessageQueue();
        String topic = queue.getTopic();
        // 暴力地扔出整個list，不讓消息阻塞
        List violentList;
        if (topic.equals(RaceConfig.MqPayTopic)) {
            violentList = new ArrayList<PaymentTuple>();
            for (MessageExt msg : msgList) {
                byte[] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("Got the end signal for " + topic + " message queue.");
                    continue;
                }

                PaymentTuple payment = new PaymentTuple(RaceUtils.readKryoObject(PaymentMessage.class, body));
                LOG.info("Get payment: " + payment.toString());
                violentList.add(payment);
            }
            collector.emit(new Values(violentList,null));   //payment
           // collector.emit(RaceConfig.HASH_STREAM, new Values(payment.getOrderId(), payment, null));

        } else {
            violentList = new ArrayList<OrderTuple>();
            for (MessageExt msg : msgList) {
                byte[] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("Got the end signal for " + topic + " message queue.");
                    continue;
                }

                OrderTuple order = new OrderTuple(RaceUtils.readKryoObject(OrderMessage.class, body).getOrderId(),
                        (short) (topic.equals(RaceConfig.MqTaobaoTradeTopic) ? 0 : 1));
                LOG.info("Get order: " + order.toString());
                violentList.add(order);
            }
            collector.emit(new Values(null, violentList));  //order message
           // collector.emit(RaceConfig.HASH_STREAM, new Values(order.getOrderId(), null, order));
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("payment", "order"));
        //declarer.declareStream(RaceConfig.HASH_STREAM, new Fields("orderId", "payment", "order"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void close() {
        if (consumer != null)
            consumer.shutdown();

        LOG.info("Conusmer shutdown ! ");
    }

    @Override
    public void activate() {
        if (consumer != null)
            consumer.resume();

        LOG.info("Conusmer activate! ");
    }

    @Override
    public void deactivate() {
        if (consumer != null)
            consumer.suspend();

        LOG.info("Conusmer deactivate! ");
    }

    @Override
    public void ack(Object o) {
        LOG.info("HashSpout ack " + o);
    }

    @Override
    public void fail(Object o) {
        LOG.info("HashSpout fail !!  " + o);
    }
}
