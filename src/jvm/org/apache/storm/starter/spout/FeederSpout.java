package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.AckFailDelegate;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.InprocMessaging;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * User: shijingui
 * Date: 2016/11/17
 */
public class FeederSpout extends BaseRichSpout {
    private int id;
    private Fields outFields;
    private SpoutOutputCollector collector;
    private AckFailDelegate ackFailDelegate;

    public FeederSpout(Fields outFields) {
        id = InprocMessaging.acquireNewPort();
        this.outFields = outFields;
    }

    public void setAckFailDelegate(AckFailDelegate d) {
        ackFailDelegate = d;
    }

    public void feed(List<Object> tuple) {
        feed(tuple, UUID.randomUUID().toString());
    }

    public void feed(List<Object> tuple, Object msgId) {
        InprocMessaging.sendMessage(id, new Values(tuple, msgId));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
        List<Object> toEmit = (List<Object>) InprocMessaging.pollMessage(id);
        System.out.println("--------------执行----------------");
        if (toEmit != null) {
            List<Object> tuple = (List<Object>) toEmit.get(0);
            Object msgId = toEmit.get(1);
            System.out.println("--------------tuple size:::" + tuple.size());
            collector.emit(tuple, msgId);
        } else {
            try {
                System.out.println("--------------spout没有数据----------------");
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void ack(Object msgId) {
        if (ackFailDelegate != null) {
            ackFailDelegate.ack(msgId);
        }
    }

    public void fail(Object msgId) {
        if (ackFailDelegate != null) {
            ackFailDelegate.fail(msgId);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outFields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}
