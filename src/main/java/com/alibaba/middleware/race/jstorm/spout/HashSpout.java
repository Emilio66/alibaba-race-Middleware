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

        consumer = ConsumerFactory.getInstance("JStormUtils.process_pid()");

        try {
            consumer.subscribe(RaceConfig.MqPayTopic, "*"); //订阅支付消息的所有tag *
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*"); //订阅淘宝订单消息
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*"); //订阅天猫订单消息

            consumer.registerMessageListener(this); //设置消息监听器, consumeMessage()实现消息处理逻辑
            consumer.start();       //!!启动consumer, 一定不能缺少！

            LOG.info("successfully create consumer " + consumer.getInstanceName());
            LOG.info("consumer nameServerAddress: " + consumer.getNamesrvAddr());
        } catch (MQClientException e) {
            LOG.error("Failed to create Consumer subscription ", e);
            e.printStackTrace();
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

        for (MessageExt msg : msgList) {
            byte[] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                LOG.info("Got the end signal for " + topic + " message queue.");
                continue;
            }

            if (topic.equals(RaceConfig.MqPayTopic)) {
                PaymentTuple payment = new PaymentTuple(RaceUtils.readKryoObject(PaymentMessage.class, body));
                collector.emit(RaceConfig.HASH_STREAM, new Values(payment.getOrderId(), payment, null));
            } else {
                OrderTuple order = new OrderTuple(RaceUtils.readKryoObject(OrderMessage.class, body).getOrderId(),
                        (short)(topic.equals(RaceConfig.MqTaobaoTradeTopic) ? 0 : 1));
                collector.emit(RaceConfig.HASH_STREAM, new Values(order.getOrderId(), null, order));
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.HASH_STREAM, new Fields("orderId", "payment", "order"));
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
