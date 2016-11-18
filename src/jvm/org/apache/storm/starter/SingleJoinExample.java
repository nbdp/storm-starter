/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.SingleJoinBolt;
import org.apache.storm.starter.spout.FeederSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;


public class SingleJoinExample {
    public static void main(String[] args) {
        FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));
        for (int i = 0; i < 10; i++) {
            String gender;
            if (i % 2 == 0) {
                gender = "male";
            } else {
                gender = "female";
            }
            genderSpout.feed(new Values(i, gender));
        }

        for (int i = 9; i >= 0; i--) {
            ageSpout.feed(new Values(i, i + 20));
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("gender", genderSpout);
        builder.setSpout("age", ageSpout);
        builder.setBolt("join", new SingleJoinBolt(new Fields("gender", "age"))).fieldsGrouping("gender", new Fields("id"))
                .fieldsGrouping("age", new Fields("id"));
        builder.setBolt("output", new JoinOutput()).shuffleGrouping("join");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-example", conf, builder.createTopology());


        Utils.sleep(10000);
        cluster.shutdown();
    }

    public static class JoinOutput extends BaseRichBolt {

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }


        @Override
        public void execute(Tuple input) {
            System.out.println("============================================");

            String gender = input.getStringByField("gender");
            int age = input.getIntegerByField("age");
            System.out.println("gender:" + gender + ", " + "age：" + age);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
}
