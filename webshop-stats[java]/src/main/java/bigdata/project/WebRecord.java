package bigdata.project;

import org.apache.kafka.streams.KeyValue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
public class WebRecord {
    private final List<String> record;
    private final String recordStr;
    private final Integer PURCHASES_INDEX = 5, PRICE_INDEX = 6, DATE_INDEX = 1;
    WebRecord(String record){
        this.recordStr = record;
        this.record = Arrays.asList(record.split(","));
    }
    public KeyValue<Long, String> getDatedRecord(){
        return new KeyValue<>(Long.parseLong(this.record.get(DATE_INDEX)),this.recordStr);
    }
    public KeyValue<String, String> getCountPair(Integer keyIndex){
        Long val = 1L;
        return new KeyValue<>(this.record.get(keyIndex),val.toString());
    }
    public KeyValue<String, String> getPurchasesCount(Integer keyIndex){
        return new KeyValue<>(this.record.get(keyIndex), this.record.get(PURCHASES_INDEX));
    }
    public KeyValue<String, String> getPurchasesValue(Integer keyIndex){
        Long val = (Long.parseLong(this.record.get(PRICE_INDEX))
                * Long.parseLong(this.record.get(PURCHASES_INDEX)));
        return new KeyValue<>(this.record.get(keyIndex),val.toString());
    }
}
