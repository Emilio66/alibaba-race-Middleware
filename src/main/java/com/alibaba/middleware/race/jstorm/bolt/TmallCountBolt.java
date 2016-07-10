package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhaoz on 2016/7/2.
 * 并发存入统计数据到hashMap中， 定时存入 Tair
 */
public class TmallCountBolt implements IRichBolt{

    private static final Logger Log = Logger.getLogger(TmallCountBolt.class);
    private OutputCollector collector;
    private Set<Long> orderSet;
    private TairOperatorImpl tairOperator;
    private String prefix;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        tairOperator = TairOperatorImpl.newInstance();
        prefix = RaceConfig.prex_tmall;
    }

    @Override
    public void execute(Tuple tuple) {
        String topic = tuple.getSourceStreamId();

        if (topic.equals(RaceConfig.tmallStream)) {
            Collection<Long> taobaoOrder = (Collection<Long>) tuple.getValue(0);
            orderSet.addAll(taobaoOrder);
        } else { // topic == RaceConfig.payStream
            Collection<PaymentTuple> payments = (Collection<PaymentTuple>) tuple.getValue(0);

            for (PaymentTuple payment : payments) {
                if (orderSet.contains(payment.getOrderId())) {
                    collector.emit(RaceConfig.TMALL_DISPATCH_STREAM, new Values(payment.getCreateTime(), payment.getPayAmount()));
                }
            }
        }

        //collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("minute", "payment"));
        declarer.declareStream(RaceConfig.TMALL_DISPATCH_STREAM,new Fields("minute", "payment"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}


