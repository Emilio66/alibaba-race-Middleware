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
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/2.
 * 把消息分解，抽取 '时间' 和 '价格' 发送到 统计 bolt
 */
public class TmallDispatchBolt implements IRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(TmallDispatchBolt.class);
    //unique payment set, same hashcode but not equal
   // private Set<PaymentTuple> distinctSet = new HashSet<PaymentTuple>();
    private int count=0;
    private long currentTime = 0L;
    private long currentMoney = 0L;
    private String prefix = RaceConfig.prex_tmall;
    private TairOperatorImpl tairOperator;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //cast to payment tuple
        PaymentTuple payment = (PaymentTuple) tuple.getValue(0);
        LOG.info("Get payment: " + payment.toString());

        long minute= payment.getCreateTime();
        long money = payment.getPayAmount();

        if(minute > currentTime && currentTime != 0){
            long savedTime = minute * 60;
            double savedMoney = currentMoney / 100.0;
            //write last minute
            tairOperator.write(prefix+"_"+savedTime, savedMoney); //存入时，保留两位小数
            currentTime = minute;
            currentMoney = 0;   //清零
            ++count;
        }
        currentMoney += money;
        if(count == 181)
            tairOperator.write(prefix+"_"+minute*60,currentMoney/100.0); //最后一分钟直接存
      //  LOG.info("TmallDispatchBolt get "+payment);

       /* if(!distinctSet.contains(payment)){
            //only emit useful fields [minute, payAmount, payPlatform] to count bolts
            collector.emit(new Values(payment.getCreateTime(), payment.getPayAmount(), payment.getPayPlatform()));
            //add to unique set
            distinctSet.add(payment);
         //   LOG.info("TmallDispatchBolt emit "+payment);
        }*/

        //collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("minute", "price", "platform"));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
