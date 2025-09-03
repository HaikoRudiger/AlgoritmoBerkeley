import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class LogicalClock {

    private final AtomicLong offsetMillis = new AtomicLong(0);


    public LogicalClock(long initialOffsetMillis) {
        offsetMillis.set(initialOffsetMillis);
    }

    public long now() {
        return System.currentTimeMillis() + offsetMillis.get();
    }

    public void adjust(long deltaMillis) {
        offsetMillis.addAndGet(deltaMillis);
    }

    public long getOffset() {
        return offsetMillis.get();
    }

    public String prettyNow() {
        SimpleDateFormat fmt = new SimpleDateFormat("HH:mm:ss.SSS");
        return fmt.format(new Date(now()));
    }

    @Override
    public String toString() {
        return "LogicalClock{now=" + prettyNow() + ", offset=" + getOffset() + " ms}";
    }
}
