package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.PersistThread;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.Utils.Arith;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/3.
 * 计算 无线/PC 历史总交易额在这一分钟时的比值
 * 0. hashSet 进行消息判重，因为这是在同一分钟当中的，因此hashSet是完整的
 * 1. 分别统计这一分钟 无线与PC 的交易额
 * 2. 将这一分钟的交易额加上历史的交易额存入 hashMap 中
 * 3. 将两个 hashMap 这一分钟的值相除，取两位小数存入 hashMap （可以在存入tair的时候统一计算ratio
 */
public class PayRatioBolt implements IRichBolt {
    private OutputCollector collector;
    public static final Logger LOG = Logger.getLogger(PayRatioBolt.class);

    private static HashMap<Long, Long> mobileMap = new HashMap<Long, Long>(); //no need for concurrent hashMap
    private static HashMap<Long, Long> pcMap = new HashMap<Long, Long>();   //calculate in place
    private static HashMap<Long, Double> ratioMap = new HashMap<Long, Double>();
    private static HashSet<PaymentTuple> distinctSet = new HashSet<PaymentTuple>(1024);
    private static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);

    private TairOperatorImpl tairOperator;
    private String prefix;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        //定时存入Tair
        /*scheduledPersist.scheduleAtFixedRate(new PersistThread(RaceConfig.prex_ratio, ratioMap),
                RaceConfig.persistInitialDelay, RaceConfig.persitInterval, TimeUnit.SECONDS);*/
        LOG.info("create bolt: " + this.toString());
        tairOperator = TairOperatorImpl.newInstance();
        prefix = RaceConfig.prex_ratio;
    }

    /**
     * 重写逻辑，根据field 直接影射到task, 同一分钟的消息在一个task中进行判重更加合理
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {

        //按照field 顺序得到payment 内容
        long orderId = tuple.getLong(0);
        long payAmount = tuple.getLong(1);
        short paySource = tuple.getShort(2);
        short platform = tuple.getShort(3);
        long createTime = tuple.getLong(4);

        LOG.info("PayRatioBolt get [order ID: " + orderId + ", time: " + createTime
                + " $" + payAmount + " ]");

        //判重
        PaymentTuple paymentTuple = new PaymentTuple(orderId, payAmount, paySource, platform, createTime);

        if (!distinctSet.contains(paymentTuple)) {
            //pc
            if (platform == 0) {
                Long pcAmount = pcMap.get(createTime);
                if (pcAmount == null) {

                    //上一分钟的历史交易额作为起点
                    if ((pcAmount = pcMap.get(createTime - 60)) == null) {
                        pcAmount = 0L;
                    }

                }

                pcAmount += payAmount; //加上历史交易作为总交易额
                pcMap.put(createTime, pcAmount);

                //计算比值
                Long mobileAmount = mobileMap.get(createTime);
                if (mobileAmount == null)
                    mobileAmount = 0L;
                //double ratio = Arith.div(mobileAmount * 1.0, pcAmount * 1.0, 2);//精确除法,保留2位
                double ratio = mobileAmount/pcAmount;
                ratioMap.put(createTime, ratio);

                tairOperator.write(prefix + "_" + createTime, ratio);
            } else {
                //无线端交易
                Long mobileAmount = mobileMap.get(createTime);
                if (mobileAmount == null) {

                    //上一分钟的历史交易额作为起点
                    if ((mobileAmount = mobileMap.get(createTime - 60)) == null) {
                        mobileAmount = 0L;
                    }
                }

                mobileAmount += payAmount; //加上历史交易作为总交易额
                mobileMap.put(createTime, mobileAmount);

                //计算比值
                Long pcAmount = pcMap.get(createTime);
                Double ratio = Double.MAX_VALUE; //pc 端为 0， 比值无限大
                if (pcAmount != null) {
                   // ratio = Arith.div(mobileAmount * 1.0, pcAmount * 1.0, 2);//精确除法,保留2位
                    ratio = mobileAmount * 1.0 / pcAmount;
                }

                ratioMap.put(createTime, ratio);
                tairOperator.write(prefix + "_" +createTime, ratio);
            }

            distinctSet.add(paymentTuple);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
