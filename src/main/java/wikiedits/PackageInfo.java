package wikiedits;

/**
 * Created by ZalaCheung on 6/27/17.
 */
public class PackageInfo {
    long timeStamp;
    String packageVersion;
    int success;
    int failure;

    public PackageInfo(long timeStamp,String packageVersion, int success,int failure){
        this.timeStamp = timeStamp;
        this.packageVersion = packageVersion;
        this.success = success;
        this.failure = failure;

    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getPackageVersion() {
        return packageVersion;
    }

    public int getSuccess() {
        return success;
    }

    public int getFailure() {
        return failure;
    }
}
