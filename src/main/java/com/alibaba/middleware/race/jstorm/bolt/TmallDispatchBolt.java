package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/2.
 * 把消息分解，抽取 '时间' 和 '价格' 发送到 统计 bolt
 */
public class TmallDispatchBolt implements IRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(TmallDispatchBolt.class);
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

        LOG.info("TmallDispatchBolt get [order ID: "+ orderId +", time: "+createTime
                +" ￥"+payAmount+" ]");
        //同一个订单，不同的payment的hashcode (hint: 生产数据payAmount小于100， 扩大paySource 与 platform比重, 不保证绝对正确
        long hashCode = payAmount | (paySource << 10) | (platform << 11) | createTime;

        //去重
        Long existOrder = uniqueMap.get(orderId);
        if(existOrder == null || existOrder != hashCode){
            collector.emit(new Values(createTime, payAmount));
            uniqueMap.put(orderId, hashCode);
            LOG.info("TmallDispatchBolt emit [order ID: "+ orderId +", time: "+createTime
                    +" ￥"+payAmount+" ]");
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
