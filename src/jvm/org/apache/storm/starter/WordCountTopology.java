package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        //topology
        TopologyBuilder builder = new TopologyBuilder();
        //spout
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        //bolt
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 2).fieldsGrouping(("split"), new Fields("word"));
        builder.setBolt("report", new WordCountReport(), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
//            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);
            cluster.killTopology("word-count");
            cluster.shutdown();
        }
    }

    /**
     * 语句拆分
     */
    public static class SplitSentence extends BaseBasicBolt {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getStringByField("word");
            String[] words = word.split(" ");
            for (String w : words) {
                collector.emit(new Values(w));
            }
        }
    }

    /**
     * 单词统计bolt
     */
    public static class WordCount extends BaseBasicBolt {
        Map<String, Long> counts = new HashMap<String, Long>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Long count = counts.get(word);
            if (count == null)
                count = 0L;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCountReport extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getStringByField("word");
            Long count = input.getLongByField("count");
            System.out.println("===================【" + word + "】" + ":::" + count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //not output
        }

    }
}
