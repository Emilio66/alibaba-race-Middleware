package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.*;
import com.alibaba.middleware.race.jstorm.spout.InputSpout;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.LocalCluster;

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
        // conf.put("user.defined.logback.conf", "classpath:logback.xml");
        int spout_Parallelism_hint = 1;
        int dispatch_Parallelism_hint = 1;
        int count_Parallelism_hint = 7;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RaceConfig.InputSpoutNsame, new InputSpout(), spout_Parallelism_hint);

        //tmall data process
        builder.setBolt("tmallDispatch", new TmallDispatchBolt(), dispatch_Parallelism_hint).
              localOrShuffleGrouping("hash", InputSpout.tmallStream);    //hash bolt emits different streams
        builder.setBolt("tmallCount", new TmallCountBolt(), count_Parallelism_hint).
                fieldsGrouping("tmallDispatch", new Fields("minute"));

        //taobao data process
        builder.setBolt("taobaoDispatch", new TaobaoDispatchBolt(), dispatch_Parallelism_hint).
              localOrShuffleGrouping("hash", InputSpout.taobaoStream);
        builder.setBolt("taobaoCount", new TaobaoCountBolt(), count_Parallelism_hint).
                fieldsGrouping("taobaoDispatch", InputSpout.taobaoStream, new Fields("minute"));

        //pay ratio process (receive two streams: tmall stream, taobao stream, field grouping by minute)
        //builder.setBolt("payDispatch", new PayDispatchBolt(), dispatch_Parallelism_hint).
        //      localOrShuffleGrouping("source", InputSpout.payStream);
        builder.setBolt("payRatioCount", new PayRatioBolt(), count_Parallelism_hint).
                fieldsGrouping("taobaoDispatch", new Fields("minute")).
                fieldsGrouping("tmallDispatch", new Fields("minute"));
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
