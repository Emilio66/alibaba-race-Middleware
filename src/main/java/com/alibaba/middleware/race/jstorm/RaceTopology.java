package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.*;
import com.alibaba.middleware.race.jstorm.spout.HashSpout;
import com.alibaba.middleware.race.jstorm.spout.InputSpout;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.LocalCluster;

import java.util.HashMap;
import java.util.Map;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = Logger.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.put("TOPOLOGY_WORKERS",4);
        conf.setNumAckers(0);   //no ack
        // conf.put("user.defined.logback.conf", "classpath:logback.xml");
        //int dispatch_Parallelism_hint = 1;

        int hash_spout_parallelism_hint = 4;
        int hash_bolt_parallelism_hint = 6;
        int dispatch_bolt_parallelism = 2;
        int count_Parallelism_hint = 4;
        int middle_bolt_parallelism =2;

        TopologyBuilder builder = new TopologyBuilder();


        SpoutDeclarer spout = builder.setSpout(RaceConfig.InputSpoutName, new HashSpout(), hash_spout_parallelism_hint);
        // force spout to run on different worker
        //Map spoutConfig = new HashMap();
        //ConfigExtension.setTaskOnDifferentNode(spoutConfig, true);
        //spout.addConfigurations(spoutConfig);

        builder.setBolt("violentRatio", new ViolentRatioBolt(),1).shuffleGrouping(RaceConfig.InputSpoutName);
        builder.setBolt("ratioSave", new RatioSaveBolt(),1).shuffleGrouping("violentRatio");


        builder.setBolt("middle", new MiddleBolt(), middle_bolt_parallelism).
                shuffleGrouping(RaceConfig.InputSpoutName);
        builder.setBolt(RaceConfig.HashBoltName, new HashBolt(), hash_bolt_parallelism_hint).setNumTasks(1)
                .fieldsGrouping("middle", RaceConfig.HASH_STREAM, new Fields("orderId"));

        //tmall data process
        builder.setBolt(RaceConfig.TMDispatchBoltName, new TmallDispatchBolt(), dispatch_bolt_parallelism).setNumTasks(1).
              localOrShuffleGrouping(RaceConfig.HashBoltName, RaceConfig.TMALL_DISPATCH_STREAM);//hash bolt emits different streams
        builder.setBolt(RaceConfig.TMCountBoltName, new TmallCountBolt(), count_Parallelism_hint).setNumTasks(1).
                fieldsGrouping(RaceConfig.TMDispatchBoltName, new Fields("minute"));

        //taobao data process
        builder.setBolt(RaceConfig.TBDispatchBoltName, new TaobaoDispatchBolt(), dispatch_bolt_parallelism).setNumTasks(1).
              localOrShuffleGrouping(RaceConfig.HashBoltName, RaceConfig.TAOBAO_DISPATCH_STREAM);
        builder.setBolt(RaceConfig.TBCountBoltName, new TaobaoCountBolt(), count_Parallelism_hint).setNumTasks(1).
                fieldsGrouping(RaceConfig.TBDispatchBoltName, new Fields("minute"));

        //give up ratio
        //pay ratio process (receive two streams: tmall stream, taobao stream, field grouping by minute)
        //builder.setBolt(RaceConfig.RatioCountBoltName, new PayRatioBolt(), 1).setNumTasks(1).
           //     fieldsGrouping(RaceConfig.TBDispatchBoltName, new Fields("minute")).
            //    fieldsGrouping(RaceConfig.TMDispatchBoltName, new Fields("minute"));

        try {
            String topologyName = RaceConfig.JstormTopologyName;
               StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            // TairClient 2.3.5: init config failed.submitTopology(topologyName, conf, builder.createTopology());
            LOG.info("Succesfully start topology with config: " + conf);
        } catch (Exception e) {
            LOG.info("Submit topology error!!", e);
            e.printStackTrace();
        }
    }
}
