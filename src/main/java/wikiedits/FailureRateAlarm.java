package wikiedits;

/**
 * Created by ZalaCheung on 6/28/17.
 */
public class FailureRateAlarm {
    long timestamp;
    String packageVersion;
    float failureRate;

    public FailureRateAlarm(long timestamp, String packageVersion,float failureRate){
        this.timestamp  = timestamp;
        this.packageVersion = packageVersion;
        this.failureRate = failureRate;
    }

    public long getTimestamp() {
        return timestamp;
    }
    public String getPackageVersion() {
        return packageVersion;
    }

    public float getFailureRate() {
        return failureRate;
    }
}
