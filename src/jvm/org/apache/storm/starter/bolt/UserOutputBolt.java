package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * User: shijingui
 * Date: 2016/11/17
 */
public class UserOutputBolt extends BaseRichBolt {

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }


    public void execute(Tuple input) {
        String nickName = input.getStringByField("nickName");
        String password = input.getStringByField("password");

        System.out.println("nickName:" + nickName + ",password:" + password);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
