package no.basis.integrationplatform.camel;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/** Captures the records one class logs through the test's JBoss LogManager. */
public final class LogCapture implements AutoCloseable {
    private final Logger logger;
    private final List<LogRecord> records = new ArrayList<>();
    private final Handler handler = new Handler() {
        @Override public synchronized void publish(LogRecord record) { records.add(record); }
        @Override public void flush() { }
        @Override public void close() { }
    };

    private LogCapture(Class<?> source) {
        logger = Logger.getLogger(source.getName());
        logger.addHandler(handler);
    }

    public static LogCapture of(Class<?> source) {
        return new LogCapture(source);
    }

    public synchronized List<LogRecord> records() {
        return List.copyOf(records);
    }

    /** Records at the level, compared by value: JBoss ERROR equals SEVERE. */
    public List<LogRecord> at(Level level) {
        return records().stream().filter(record -> record.getLevel().intValue() == level.intValue()).toList();
    }

    /** The message with its arguments; formats differ between logging providers. */
    public static String text(LogRecord record) {
        String text = record.getMessage();
        Object[] parameters = record.getParameters();
        if (parameters != null) {
            for (Object parameter : parameters) {
                text = text.replaceFirst("\\{}", java.util.regex.Matcher.quoteReplacement(String.valueOf(parameter)));
            }
        }
        return text;
    }

    @Override
    public void close() {
        logger.removeHandler(handler);
    }
}
