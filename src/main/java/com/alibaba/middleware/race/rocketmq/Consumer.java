package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class Consumer {
    private static String groupname="singularity";
    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupname);

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //在本地搭建好broker后,记得指定nameServer的地址
        //consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.subscribe(RaceConfig.MqPayTopic, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {

                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        //Info: 生产者停止生成数据, 并不意味着马上结束？？ 什么时候结束？
                        System.out.println("Got the end signal");
                        continue;
                    }

                    PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                    System.out.println(paymentMessage); //输出收到的支付消息
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
