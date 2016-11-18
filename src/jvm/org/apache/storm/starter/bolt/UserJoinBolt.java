package org.apache.storm.starter.bolt;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TimeCacheMap;

import java.util.*;

/**
 * User: shijingui
 * Date: 2016/11/17
 */
public class UserJoinBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Fields outFields;
    Map<String, GlobalStreamId> fieldLocations;
    Fields idFields;
    int numSources;
    TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>> pending;


    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        fieldLocations = new HashMap<String, GlobalStreamId>();
        this.collector = collector;
        int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        pending = new TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>>(timeout, new ExpireCallback());
        numSources = context.getThisSources().size();
        Set<String> idFields = null;
        for (GlobalStreamId source : context.getThisSources().keySet()) {
            Fields fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
            Set<String> setFields = new HashSet<String>(fields.toList());
            if (idFields == null)
                idFields = setFields;
            else
                idFields.retainAll(setFields);

            for (String outfield : outFields) {
                for (String sourceField : fields) {
                    if (outfield.equals(sourceField)) {
                        fieldLocations.put(outfield, source);
                    }
                }
            }
        }
        this.idFields = new Fields(new ArrayList<String>(idFields));

        if (fieldLocations.size() != outFields.size()) {
            throw new RuntimeException("Cannot find all outfields among sources");
        }

    }

    public UserJoinBolt(Fields fields) {
        this.outFields = fields;
    }

    public void execute(Tuple tuple) {
        List<Object> ids = tuple.select(idFields);
        GlobalStreamId streamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());
        if (!pending.containsKey(ids)) {
            pending.put(ids, new HashMap<GlobalStreamId, Tuple>());
        }
        Map<GlobalStreamId, Tuple> parts = pending.get(ids);
        if (parts.containsKey(streamId)) {
            //throw new RuntimeException("Received same side of single join twice");
        } else
            parts.put(streamId, tuple);

        if (parts.size() == numSources) {
            pending.remove(ids);
            List<Object> joinResult = new ArrayList<Object>();
            for (String outField : outFields) {
                GlobalStreamId loc = fieldLocations.get(outField);
                joinResult.add(parts.get(loc).getValueByField(outField));
            }
            collector.emit(new ArrayList<Tuple>(parts.values()), joinResult);

            for (Tuple part : parts.values()) {
                collector.ack(part);
            }
        }

//        int id = tuple.getIntegerByField("id");
//        String password = tuple.getStringByField("password");
//        String nickName = tuple.getStringByField("nickName");


//        System.out.println("user join info={id:" + id + ",password:" + password + ",nickName:" + nickName + "}");

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outFields);
    }


    private class ExpireCallback implements TimeCacheMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {
        @Override
        public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {
            for (Tuple tuple : tuples.values()) {
                collector.fail(tuple);
            }
        }
    }
}
