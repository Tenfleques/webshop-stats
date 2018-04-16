package com.flequesboard.java.apps;

import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
public class WebRecord {
    private List<String> record;
    private final String recordStr;
    WebRecord(String record){
        this.recordStr = record;
        List<String> buffer = Arrays.asList(record.split(","));
        String longPlatformText = buffer.get(RecordFields.PLATFORM_FIELD.getValue());

        PlatformFormat frmt = new PlatformFormat();

        buffer.set(RecordFields.PLATFORM_FIELD.getValue(),frmt.getDevice(longPlatformText));

        this.record = new ArrayList<>(buffer);

        this.record.add(RecordFields.OS_FIELD.getValue(),frmt.getOS(longPlatformText));
        this.record.add(RecordFields.BROWSER_FIELD.getValue(),frmt.getBrowser(longPlatformText));


    }

    public List<String> getRecord() {
        return record;
    }

    private String formatFields(int key){
        if(key == RecordFields.PRICE_FIELD.getValue())
            return "P. " + this.record.get(key);
        if(key == RecordFields.DATE_FIELD.getValue())
            return new java.sql.Date(Long.parseLong(this.record.get(key))).toString();
        return this.record.get(key);
    }
    public KeyValue<Long, String> getDatedRecord(){
        return new KeyValue<>(Long.parseLong(this.record.get(RecordFields.DATE_FIELD.getValue())),this.recordStr);
    }
    public KeyValue<String, String> getCountPair(int keyIndex){
        Long val = 1L;
        return new KeyValue<>(formatFields(keyIndex),val.toString());
    }
    public KeyValue<String, String> getPurchasesCount(int keyIndex){
        return new KeyValue<>(formatFields(keyIndex), this.record.get(RecordFields.PURCHASES_FIELD.getValue()));
    }
    public KeyValue<String, String> getPurchasesValue(int keyIndex){
        Long val = (Long.parseLong(this.record.get(RecordFields.PRICE_FIELD.getValue()))
                * Long.parseLong(this.record.get(RecordFields.PURCHASES_FIELD.getValue())));
        return new KeyValue<>(formatFields(keyIndex),val.toString());
    }
}
