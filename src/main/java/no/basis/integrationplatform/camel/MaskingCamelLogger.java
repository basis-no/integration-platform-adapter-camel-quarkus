package no.basis.integrationplatform.camel;

import org.apache.camel.LoggingLevel;
import org.apache.camel.spi.CamelLogger;
import org.slf4j.LoggerFactory;

/**
 * The logger of the adapter's error handlers: Camel's own line for a failed delivery names the
 * failure's message, and so a receiver's URI with its credentials. Each line and failure is
 * masked by {@link ErrorTextMasking} once, before it is written, under the logger name Camel uses.
 * Every method writes its line itself instead of through another of Camel's methods, which may
 * call back into this logger and would mask the line a second time.
 */
final class MaskingCamelLogger extends CamelLogger {
    MaskingCamelLogger(String logName) {
        super(LoggerFactory.getLogger(logName), LoggingLevel.ERROR);
    }

    // Masks only a line that is written: a DEBUG retry line costs nothing while DEBUG is off.
    @Override
    public void log(String message, LoggingLevel loggingLevel) {
        if (shouldLog(getLog(), loggingLevel)) {
            write(loggingLevel, ErrorTextMasking.mask(message), null);
        }
    }

    @Override
    public void log(String message) {
        log(message, getLevel());
    }

    @Override
    public void doLog(String message) {
        write(getLevel(), ErrorTextMasking.mask(message), null);
    }

    @Override
    public void log(String message, Throwable exception, LoggingLevel loggingLevel) {
        if (shouldLog(getLog(), loggingLevel)) {
            write(loggingLevel, ErrorTextMasking.mask(message), ErrorTextMasking.forLog(exception));
        }
    }

    @Override
    public void log(String message, Throwable exception) {
        log(message, exception, getLevel());
    }

    private void write(LoggingLevel level, String message, Throwable exception) {
        if (exception != null) {
            CamelLogger.log(getLog(), level, getMarker(), message, exception);
        } else if (getMarker() != null) {
            CamelLogger.log(getLog(), level, getMarker(), message);
        } else {
            CamelLogger.log(getLog(), level, message);
        }
    }
}
