package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.UserJoinBolt;
import org.apache.storm.starter.bolt.UserOutputBolt;
import org.apache.storm.starter.spout.UserInfoSpout;
import org.apache.storm.starter.spout.UserSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * User: shijingui
 * Date: 2016/11/17
 */
public class UserJoinTopology {


    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("user", new UserSpout(), 1);
        builder.setSpout("userInfo", new UserInfoSpout(), 1);
        builder.setBolt("UserJoinBolt", new UserJoinBolt(new Fields("password", "nickName")), 1).fieldsGrouping("user", new Fields("id")).fieldsGrouping("userInfo", new Fields("id"));
        builder.setBolt("output", new UserOutputBolt(), 1).shuffleGrouping("UserJoinBolt");
        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(1);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("userJoin", config, builder.createTopology());

//        Utils.sleep(20000);
//        localCluster.killTopology("userJoin");
//        localCluster.shutdown();
//        System.out.println("------------------------------------shutdown---------------------------");

    }
}
