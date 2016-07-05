package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by Huiyi on 2016/7/4.
 */
public class InputSpout implements IRichSpout, MessageListenerConcurrently {

    private static final Logger LOG = Logger.getLogger(InputSpout.class);

    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer consumer;

    protected transient LinkedBlockingDeque<PaymentTuple> paymentBuffer;

    protected transient Set<Long> tmallOrder;
    protected transient Set<Long> taobaoOrder;

    public static String tmallStream = "tmall";
    public static String taobaoStream = "taobao";
    public static String payStream = "pay";

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        paymentBuffer = new LinkedBlockingDeque<PaymentTuple>();
        tmallOrder = new HashSet<Long>();
        taobaoOrder = new HashSet<Long>();

        String instanceName = RaceConfig.MetaConsumerGroup + "@" + JStormUtils.process_pid();
        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        consumer.setNamesrvAddr(System.getProperty("NAMESRV_ADDR"));//(RaceConfig.nameServer);
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try {
            consumer.subscribe(RaceConfig.MqPayTopic, "*"); //订阅支付消息的所有tag *
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*"); //订阅淘宝订单消息
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*"); //订阅天猫订单消息

            consumer.registerMessageListener(this); //设置消息监听器, consumeMessage()实现消息处理逻辑
            consumer.start();       //!!启动consumer, 一定不能缺少！

            LOG.info("successfully create consumer " + instanceName);
        } catch (MQClientException e) {
            LOG.error("Failed to create Consumer subscription ", e);
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            PaymentTuple payment = paymentBuffer.take();
            sendTuple(payment);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sendTuple(PaymentTuple payment) {
        if (taobaoOrder.contains(payment.getOrderId())) {
            collector.emit(RaceConfig.taobaoStream, new Values(payment.getOrderId(), payment.getPayAmount(),
                    payment.getPaySource(), payment.getPayPlatform(), payment.getCreateTime()));
        } else if (tmallOrder.contains(payment.getOrderId())) {
            collector.emit(RaceConfig.tmallStream, new Values(payment.getOrderId(), payment.getPayAmount(),
                    payment.getPaySource(), payment.getPayPlatform(), payment.getCreateTime()));
        } else {
            paymentBuffer.offer(payment);
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        MessageQueue queue = context.getMessageQueue();
        String topic = queue.getTopic();

        if (topic.equals(RaceConfig.MqPayTopic)) {
            for (MessageExt msg : msgList) {
                byte[] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("Got the end signal of Payment message queue");
                    continue;
                }

                PaymentTuple payment = new PaymentTuple(RaceUtils.readKryoObject(PaymentMessage.class, body));

                // first emit it to payStream
                collector.emit(RaceConfig.payStream, new Values(payment.getOrderId(), payment.getPayAmount(),
                        payment.getPaySource(), payment.getPayPlatform(), payment.getCreateTime()));

                // second join with orderId to determine whether its for tmall or taobao
                sendTuple(payment);
                LOG.debug("consuemr get pay - "+msg.getTopic()+" message [order ID: "+ payment.getOrderId()
                        +", time: "+payment.getCreateTime()
                        +" ￥"+payment.getPayAmount()+" ]");
            }
        } else {
            for (MessageExt msg : msgList) {
                byte[] body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    LOG.info("Got the end signal of Taobao message queue");
                    continue;
                }

                OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
                    taobaoOrder.add(order.getOrderId());
                    LOG.debug("consuemr get taobao - "+msg.getTopic()+" message [order ID: "+ order.getOrderId()
                            +" ]");
                } else {
                    tmallOrder.add(order.getOrderId());
                    LOG.debug("consuemr get tmall- "+msg.getTopic()+" message [order ID: "+ order.getOrderId()
                            +" ]");
                }
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void close() {
        if (consumer != null)
            consumer.shutdown();
    }

    @Override
    public void activate() {
        if (consumer != null)
            consumer.resume();
    }

    @Override
    public void deactivate() {
        if (consumer != null)
            consumer.suspend();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declare 3 stream
        declarer.declareStream(taobaoStream, new Fields(taobaoStream));
        declarer.declareStream(tmallStream, new Fields(tmallStream));
        declarer.declareStream(payStream, new Fields(payStream));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
