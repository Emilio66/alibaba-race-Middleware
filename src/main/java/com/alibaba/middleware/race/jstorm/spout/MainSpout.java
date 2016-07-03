package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.MsgTuple;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by zhaoz on 2016/6/27.
 * 1. 持续拉取 RocketMQ 消息（总共三个topic: 天猫消息，淘宝消息，付款消息）
 * 2. 将订单消息按照购物平台分发：以 prex_tmall, prex_taobao 作为 outputFiled, 交给相应的bolt统计每分钟交易额
 * 3. 将支付消息按照支付平台分发：以pc, mobile 作为outputField，相应bolt进行统计，最后求ratio
 */
public class MainSpout implements IRichSpout, MessageListenerConcurrently {

    private static final long serialVersionUID = 829732194381L;
    private static final Logger Log = Logger.getLogger(MainSpout.class);

    protected boolean flowControl;  //流量控制，消息缓存与否
    protected boolean autoACK;      //是否使用默认的ACK, 后续实现ACKer
    protected String id;
    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer consumer; //pushConsumer封装了pull轮询，定制可用原生pullConsumer
    protected transient LinkedBlockingDeque<MsgTuple> tmallQueue; //消息缓冲队列
    protected transient LinkedBlockingDeque<MsgTuple> taobaoQueue;
    protected transient LinkedBlockingDeque<MsgTuple> payQueue;

    public static String tmallStream = "tmall";
    public static String taobaoStream = "taobao";
    public static String payStream = "pay";

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        id = topologyContext.getThisComponentId() + ":" + topologyContext.getThisTaskId();
        collector = spoutOutputCollector;
        tmallQueue = new LinkedBlockingDeque<MsgTuple>();
        taobaoQueue = new LinkedBlockingDeque<MsgTuple>();
        payQueue = new LinkedBlockingDeque<MsgTuple>();
        flowControl = RaceConfig.isFlowControl;
        autoACK = RaceConfig.autoACK;

        //log
        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init MetaSpout:").append(id);
        Log.info(sb.toString());

        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        consumer.setNamesrvAddr(System.getProperty("NAMESRV_ADDR"));//(RaceConfig.nameServer);

        String instanceName = RaceConfig.MetaConsumerGroup + "@" + JStormUtils.process_pid();
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);//设置第一次消费位置，非第一次接上次位置
        try {
            consumer.subscribe(RaceConfig.MqPayTopic, "*"); //订阅支付消息的所有tag *
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*"); //订阅淘宝订单消息
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*"); //订阅天猫订单消息

            consumer.registerMessageListener(this); //设置消息监听器, consumeMessage()实现消息处理逻辑
            consumer.start();       //!!启动consumer, 一定不能缺少！

            Log.info("successfully create consumer " + instanceName);
        } catch (MQClientException e) {
            Log.error("Failed to create Consumer subscription ", e);
            e.printStackTrace();
        }

        /**
         * 以备调优使用，自定义参数
         consumer.setPullThresholdForQueue(config.getQueueSize());
         consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
         consumer.setPullBatchSize(config.getPullBatchSize());
         consumer.setPullInterval(config.getPullInterval());
         consumer.setConsumeThreadMin(config.getPullThreadNum());
         consumer.setConsumeThreadMax(config.getPullThreadNum());
         */

    }


    //发消息
    public void sendTuple(MsgTuple tupleList, String streamID) {
        tupleList.updateEmitTime();
        collector.emit(streamID, new Values(tupleList), tupleList.getCreateTime()); //emit tuples to bolts
    }

    //spout 关键函数 nextTuple, 发送 tuple 给 bolt
    @Override
    public void nextTuple() {
        MsgTuple tmallTuple = null, taobaoTuple = null, payTuple = null;
        try {
            tmallTuple = tmallQueue.take();     //从队列中取消息
            taobaoTuple = taobaoQueue.take();
            payTuple = payQueue.take();

        } catch (InterruptedException e) {
            Log.error("Failed to pull message", e);
            e.printStackTrace();
        }

        if (tmallTuple != null)
            sendTuple(tmallTuple, tmallStream);
        if (taobaoTuple != null)
            sendTuple(taobaoTuple, taobaoStream);
        if (payTuple != null)
            sendTuple(payTuple, payStream);
    }

    /**
     * 消息监听并处理
     *
     * @param msgList
     * @param context
     * @return status
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        MessageQueue queue = context.getMessageQueue();
        String topic = queue.getTopic();        //每个topic 对应若干条queue, 每个queue有一个messageList，Q：是否要逐条检查？(可以试一下)
        MsgTuple msgTuple = new MsgTuple(msgList, queue); //构造 tuple stream

        if(flowControl) {
            if (topic.equals(RaceConfig.MqPayTopic)) {
                payQueue.offer(msgTuple);       //offer 立即加入双端队列尾部，与add不同，它不抛出异常而是返回特殊值

            } else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
                taobaoQueue.offer(msgTuple);

            } else if (topic.equals(RaceConfig.MqTmallTradeTopic)) {
                tmallQueue.offer(msgTuple);
            }
        }
        else {
            if (topic.equals(RaceConfig.MqPayTopic)) {
                sendTuple(msgTuple, payStream);

            } else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
                sendTuple(msgTuple, taobaoStream);

            } else if (topic.equals(RaceConfig.MqTmallTradeTopic)) {
                sendTuple(msgTuple, tmallStream);
            }
        }

        if (autoACK) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } else {
            try {
                msgTuple.waitFinish();  //等待ACK
            } catch (InterruptedException e) {
                Log.error("Failed to ACK .. ", e);
                e.printStackTrace();
            }
            if (msgTuple.isSuccess() == true) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
    }

    //发出后的标识
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //三种类型的消息用不同的field, 创建三条stream
        declarer.declareStream(taobaoStream, new Fields(taobaoStream)); //fields 映射 emit 中的 values
        declarer.declareStream(tmallStream, new Fields(tmallStream));
        declarer.declareStream(payStream, new Fields(payStream));

        //declarer.declare(new Fields("tuple"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    @Override
    public void close() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Override
    public void activate() {
        if (consumer != null) {
            consumer.resume();
        }
    }

    @Override
    public void deactivate() {
        if (consumer != null) {
            consumer.suspend();
        }
    }


    @Override
    public void ack(Object o) {
        //do something
    }

    @Override
    public void fail(Object o) {
        //do something
    }

    /*@Override
    public void fail(Object msgId, List<Object> values) {
        MsgTuple metaTuple = (MsgTuple) values.get(0);
        AtomicInteger failTimes = metaTuple.getFailureTimes();

        int failNum = failTimes.incrementAndGet();
        if (failNum > RaceConfig.maxFailTime) {
            Log.warn("Message " + metaTuple.getMq() + " fail times " + failNum);
            metaTuple.done();
            return;
        }

        if (flowControl) {
            sendingQueue.offer(metaTuple);
        } else {
            sendTuple(metaTuple);
        }
    }*/
}
