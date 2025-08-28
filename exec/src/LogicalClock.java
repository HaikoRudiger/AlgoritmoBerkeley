import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Relógio lógico simples: now() = System.currentTimeMillis() + offset.
 * Usado para simular adiantos/atrasos e aplicar ajustes.
 */
public class LogicalClock {
    private final AtomicLong offsetMillis = new AtomicLong(0);

    public LogicalClock(long initialOffsetMillis) {
        offsetMillis.set(initialOffsetMillis);
    }

    /** Tempo lógico atual em milissegundos. */
    public long now() {
        return System.currentTimeMillis() + offsetMillis.get();
    }

    /** Ajusta o clock lógico somando delta (pode ser positivo ou negativo). */
    public void adjust(long deltaMillis) {
        offsetMillis.addAndGet(deltaMillis);
    }

    /** Deslocamento atual em relação ao relógio do SO. */
    public long getOffset() {
        return offsetMillis.get();
    }

    /** String amigável do horário lógico. */
    public String prettyNow() {
        SimpleDateFormat fmt = new SimpleDateFormat("HH:mm:ss.SSS");
        return fmt.format(new Date(now()));
    }

    @Override
    public String toString() {
        return "LogicalClock{now=" + prettyNow() + ", offset=" + getOffset() + " ms}";
    }
}
