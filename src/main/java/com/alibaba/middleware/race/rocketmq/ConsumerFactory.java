package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
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

    public synchronized static DefaultMQPushConsumer getInstance(String processId) {
        DefaultMQPushConsumer consumer = consumers.get(processId);

        if (consumer == null) {
            String instanceName = RaceConfig.MetaConsumerGroup + "@" + processId;
            consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
            consumer.setInstanceName(instanceName);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            //for better perfomance, message batch process
            consumer.setConsumeMessageBatchMaxSize(100);
            consumers.put(processId, consumer);
            LOG.info("initialize a consumer at " + processId);
        }

        return consumer;
    }
}
