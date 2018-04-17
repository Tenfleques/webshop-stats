package com.flequesboard.java.apps;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class StorePairFields {
    private HashMap<Integer, String> storePairFields;
    private HashMap<String, Integer> valueKeys;
    StorePairFields(){
        storePairFields = new java.util.HashMap<>();
        valueKeys = new HashMap<>();
        storePairFields.put(RecordFields.DATE_FIELD.getValue(),"date-");
        storePairFields.put(RecordFields.PLATFORM_FIELD.getValue(),"platform-");
        storePairFields.put(RecordFields.BROWSER_FIELD.getValue(),"browser-");
        storePairFields.put(RecordFields.OS_FIELD.getValue(),"os-");
        storePairFields.put(RecordFields.REFERER_FIELD.getValue(),"referer-");
        storePairFields.put(RecordFields.ITEM_FIELD.getValue(),"item-");
        storePairFields.put(RecordFields.PRICE_FIELD.getValue(),"price-");

        Iterator<Map.Entry<Integer,String>> stat = storePairFields.entrySet().iterator();
        while(stat.hasNext()){
            Map.Entry<Integer,String> pair = stat.next();
            valueKeys.put(pair.getValue().split("-")[0],pair.getKey());
        }

    }
    public Iterator<Map.Entry<Integer,String>> getValues(){
        return storePairFields.entrySet().iterator();
    }
    public String getValue(Integer key){
        return storePairFields.get(key);
    }
    public Integer getKey(String value){
        return valueKeys.get(value);
    }
}
