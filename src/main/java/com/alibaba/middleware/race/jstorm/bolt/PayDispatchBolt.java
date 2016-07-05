package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.MsgTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/3.
 */
public class PayDispatchBolt implements IRichBolt{
    private OutputCollector collector;
    private static final Logger Log = Logger.getLogger(PayDispatchBolt.class);
    private static ConcurrentHashMap<Long, Long> uniqueMap= new ConcurrentHashMap<Long, Long>(1024);;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //按照field 顺序得到payment 内容
        long orderId = tuple.getLong(0);
        long payAmount = tuple.getLong(1);
        short paySource = tuple.getShort(2);
        short platform = tuple.getShort(3);
        long createTime = tuple.getLong(4);

        Log.debug("PayDispatchBolt get [order ID: "+ orderId +", time: "+createTime
                +" ￥"+payAmount+" ]");
        //同一个订单，不同的payment的hashcode (hint: 生产数据payAmount小于100， 扩大paySource 与 platform比重, 不保证绝对正确
        long hashCode = payAmount | (paySource << 10) | (platform << 11) | createTime;

        //去重
        Long existOrder = uniqueMap.get(orderId);
        if(existOrder == null || existOrder != hashCode){
            collector.emit(new Values(createTime, payAmount, platform));
            uniqueMap.put(orderId, hashCode);
            Log.debug("PayDispatchBolt emit [order ID: "+ orderId +", time: "+createTime
                    +" ￥"+payAmount+" ]");
        }
        /*MsgTuple msgTuple = (MsgTuple) tuple;

        //处理每条消息, 同一分钟的消息派发到同一个task（线程）当中
        for (MessageExt msg : msgTuple.getMsgList()) {
            byte[] body = msg.getBody();

            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                Log.info("Got the end signal of Payment message queue");
                continue;
            }

            PaymentMessage payMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);

            //需要订单时间、平台、金额，后续可能需要 orderID 来去重？
            double amount = payMessage.getPayAmount();
            short platform = payMessage.getPayPlatform();
            long second = payMessage.getCreateTime() / 1000;
            long minute = (second / 60) * 60;   //以第0秒作为这一分钟的标识，10位

            collector.emit(new Values(minute, amount, platform));
        }*/

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("minute", "amount", "platform"));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
