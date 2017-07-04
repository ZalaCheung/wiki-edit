package wikiedits;

/**
 * Created by ZalaCheung on 7/4/17.
 */
public class TestEvent {
    String eventType;
    int value;
    long timeStamp;

    TestEvent(String eventType,int value){
        this.eventType = eventType;
        this.value = value;
        this.timeStamp = System.currentTimeMillis();
    }
    public String getEventType(){
        return eventType;
    }

    public int getValue() {
        return value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
