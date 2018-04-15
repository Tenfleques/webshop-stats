package bigdata.project;

import org.apache.kafka.streams.KeyValue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
public class WebRecord {
    private final List<String> record;
    private final String recordStr;
    WebRecord(String record){
        this.recordStr = record;
        this.record = Arrays.asList(record.split(","));
    }
    private String cureIfDateField(int key){
        if(key == RecordFields.DATE_FIELD.getValue())
            return new java.sql.Date(Long.parseLong(this.record.get(key))).toString();
        return this.record.get(key);
    }
    public KeyValue<Long, String> getDatedRecord(){
        return new KeyValue<>(Long.parseLong(this.record.get(RecordFields.DATE_FIELD.getValue())),this.recordStr);
    }
    public KeyValue<String, String> getCountPair(int keyIndex){
        Long val = 1L;
        return new KeyValue<>(cureIfDateField(keyIndex),val.toString());
    }
    public KeyValue<String, String> getPurchasesCount(int keyIndex){
        return new KeyValue<>(cureIfDateField(keyIndex), this.record.get(RecordFields.PURCHASES_FIELD.getValue()));
    }
    public KeyValue<String, String> getPurchasesValue(int keyIndex){
        Long val = (Long.parseLong(this.record.get(RecordFields.PRICE_FIELD.getValue()))
                * Long.parseLong(this.record.get(RecordFields.PURCHASES_FIELD.getValue())));
        return new KeyValue<>(cureIfDateField(keyIndex),val.toString());
    }
}
