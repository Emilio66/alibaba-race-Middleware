package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by zhaoz on 2016/7/10.
 */
public class OrderSaveBolt implements IRichBolt {
    public static final Logger LOG = Logger.getLogger(OrderSaveBolt.class);
    private OutputCollector collector;

    private TairOperatorImpl tairOperator;
    private long tbCurTime = 0L;
    private long tmCurTime = 0L;
    private long tbCurAmount = 0L;
    private long tmCurAmount = 0L;
    private String tbPrefix;
    private String tmPrefix;

    private int tbCount = 0; //tricky count
    private int tmCount = 0; //tricky count
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.info("create ratio bolt: " + this.toString());
        tairOperator = TairOperatorImpl.newInstance();
        tbPrefix = RaceConfig.prex_taobao;
        tmPrefix = RaceConfig.prex_tmall;
    }

    @Override
    public void execute(Tuple tuple) {
        long minute = tuple.getLong(0); //存储时再转换成第0秒
        long money = tuple.getLong(1);  //存时转换成double

        if(tuple.getSourceStreamId().equals(RaceConfig.TAOBAO_DISPATCH_STREAM)){

            if(minute > tbCurTime && tbCurTime != 0){
                //save last minute when a new minute comes
                tairOperator.write(tbPrefix+"_"+(tbCurTime * 60), tbCurAmount/100.0);
                tbCurTime = minute;
                tbCurAmount = 0L; //clear 0
                ++tbCount;
            }
            tbCurAmount += money;
            if (tbCount == 181){
                tairOperator.write(tbPrefix+"_"+(tbCurTime * 60), tbCurAmount/100.0);
            }
        }else{

            if(minute > tmCurTime && tmCurTime != 0){
                tairOperator.write(tmPrefix+"_"+(tmCurTime * 60), tmCurAmount/100.0);
                tmCurTime = minute;
                tmCurAmount = 0L;
                ++tmCount;
            }
            tmCurAmount += money;
            if(tmCount == 181){
                tairOperator.write(tmPrefix+"_"+(tmCurTime * 60), tmCurAmount/100.0);
            }
        }
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

