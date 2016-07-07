package com.alibaba.middleware.race.rocketmq;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.log4j.Logger;

/**
 * Created by Huiyi on 2016/7/7.
 */
public class ConsumerFactory {
    private static final Logger LOG = Logger.getLogger(ConsumerFactory.class);

    private static DefaultMQPushConsumer consumer;

    public synchronized static DefaultMQPushConsumer getInstance() {
        if (consumer == null) {
            String instanceName = RaceConfig.MetaConsumerGroup + "@" + JStormUtils.process_pid();
            consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
            consumer.setInstanceName(instanceName);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            //for better perfomance, message batch process
            consumer.setConsumeMessageBatchMaxSize(100);
        }
        return consumer;
    }
}
