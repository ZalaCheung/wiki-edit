package wikiedits;

import java.util.regex.Pattern;

/**
 * Created by ZalaCheung on 7/4/17.
 */
public class PatternResult {
    long timeStamp;
    String result;

    PatternResult(String result){
        this.result = result;
        this.timeStamp = System.currentTimeMillis();
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getResult() {
        return result;
    }
}
