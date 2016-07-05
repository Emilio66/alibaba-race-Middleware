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
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/3.
 * 1. 全局缓存orderID对应的payment object, payment消息判重(hashset效率高，但可能误判),使用hashMap
 * 2. 拿到这个payment, 按分钟区分，发送给bolt去统计
 */
public class TaobaoDispatchBolt implements IRichBolt{
    private OutputCollector collector;
    private static final Logger Log = Logger.getLogger(TaobaoDispatchBolt.class);
    //每个orderID都存，数据量大可用 bloomfilter
    private static ConcurrentHashMap<Long, Long> uniqueMap= new ConcurrentHashMap<Long, Long>(1024);

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

        //同一个订单，不同的payment的hashcode (hint: 生产数据payAmount小于100， 扩大paySource 与 platform比重, 不保证绝对正确
        long hashCode = payAmount | (paySource << 10) | (platform << 11) | createTime;

        //去重
        Long existOrder = uniqueMap.get(orderId);
        if(existOrder == null || existOrder != hashCode){
            collector.emit(new Values(createTime, payAmount));
            uniqueMap.put(orderId, hashCode);
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("minute", "price"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
