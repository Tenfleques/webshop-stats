package com.flequesboard.java.apps;

enum AdministrativeStores {
    LIVE_DATES("LIVEDATES");
    String value;
    AdministrativeStores(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
