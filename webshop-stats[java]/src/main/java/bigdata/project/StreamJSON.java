package bigdata.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;

public class StreamJSON {
    String json;
    StreamJSON(KeyValueIterator<String,String> streamData){
        json = "[";
        Integer i = 0;
        while(streamData.hasNext()){
            KeyValue<String, String> record = streamData.next();
            if(i!=0)
                json += ",";
            json += "{";
            json += "\"key\":\""+record.key + "\",\"value\":" + "\""+ record.value+"\"";
            json += "}";
            i++;
        }
        json += "]";
    }
    StreamJSON(Iterator<Map.Entry<String,Long>> map){
        json = "[";
        Integer i = 0;
        while(map.hasNext()){
            Map.Entry<String, Long> record = map.next();
            if(i!=0)
                json += ",";
            json += "{";
            json += "\"key\":\""+record.getKey() + "\",\"value\":" + "\""+ record.getValue()+"\"";
            json += "}";
            i++;
        }
        json += "]";
    }
    public String getJson(){
        return json;
    }
}
