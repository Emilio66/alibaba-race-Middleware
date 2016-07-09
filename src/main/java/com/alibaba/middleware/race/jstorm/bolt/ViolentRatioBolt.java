package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by zhaoz on 2016/7/9.
 * 如果时间超过当前时间则往后发
 * 不超过就累加
 */
public class ViolentRatioBolt implements IRichBolt {
    private OutputCollector collector;
    private long currentTime = 0L;
    private long currentPC = 0L;
    private long currentWL = 0L;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Object field1 = input.getValue(0);

        //payment list tuple
        if (field1 != null) {
            ArrayList<PaymentTuple> list = (ArrayList<PaymentTuple>) field1;
            for (PaymentTuple payment : list) {
                Long time = payment.getCreateTime();
                if (time > currentTime ) {
                    //emit last minute
                    if(currentPC != 0)
                        collector.emit(new Values(currentTime, currentWL * 1.0 / currentPC));
                    currentTime = time;
                }

                if (payment.getPayPlatform() == 0) {
                    currentPC += payment.getPayAmount();
                } else
                    currentWL += payment.getPayAmount();

            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "ratio"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
