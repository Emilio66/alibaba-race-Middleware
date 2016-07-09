package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Huiyi on 2016/7/7.
 */
public class ConsumerFactory {
    private static final Logger LOG = Logger.getLogger(ConsumerFactory.class);

    private static Map<String, DefaultMQPushConsumer> consumers = new HashMap<>();

    public synchronized static DefaultMQPushConsumer getInstance(String processId, MessageListenerConcurrently listener)
            throws Exception {
        String instanceName = RaceConfig.MetaConsumerGroup + "@" + processId;
        DefaultMQPushConsumer consumer = consumers.get(instanceName);

        if (consumer != null) {
            LOG.info("Consumer of " + instanceName + " has been created, don't recreate it");
            return null;
        }

        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setPullBatchSize(100);
        consumer.setConsumeThreadMin(4);
        consumer.setConsumeThreadMax(4);

        consumer.subscribe(RaceConfig.MqPayTopic, "*"); //订阅支付消息的所有tag *
        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*"); //订阅淘宝订单消息
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*"); //订阅天猫订单消息

        consumer.registerMessageListener(listener);

        consumer.start();

        consumers.put(instanceName, consumer);

        return consumer;
    }
}
