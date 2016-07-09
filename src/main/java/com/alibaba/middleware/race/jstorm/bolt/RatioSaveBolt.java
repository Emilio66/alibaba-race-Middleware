package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaoz on 2016/7/9.
 */
public class RatioSaveBolt implements IRichBolt{
    public static final Logger LOG = Logger.getLogger(RatioSaveBolt.class);
    private OutputCollector collector;

    private TairOperatorImpl tairOperator;
    private String prefix;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.info("create ratio bolt: " + this.toString());
        tairOperator = TairOperatorImpl.newInstance();
        prefix = RaceConfig.prex_ratio;
    }

    @Override
    public void execute(Tuple tuple) {
        long createTime = tuple.getLong(0) * 60; //1st second stands for this minute
        double ratio = tuple.getDouble(1);

        //persist
        tairOperator.write(prefix + "_" + createTime, ratio);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
