package bigdata.project;

public enum  RecordFields {
    DATE_FIELD (1),
    PLATFORM_FIELD (2),
    REFERER_FIELD(3),
    ITEM_FIELD(4),
    PURCHASES_FIELD(5),
    PRICE_FIELD(6);
    private final int value;

    RecordFields(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
