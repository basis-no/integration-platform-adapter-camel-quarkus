package no.basis.integrationplatform.camel;

import org.apache.camel.LoggingLevel;
import org.apache.camel.spi.CamelLogger;
import org.slf4j.LoggerFactory;

/**
 * The logger of the adapter's error handlers: Camel's own line for a failed delivery names the
 * failure's message, and so a receiver's URI with its credentials. Each line and failure is
 * masked by {@link ErrorTextMasking} before it is written, under the logger name Camel uses.
 */
final class MaskingCamelLogger extends CamelLogger {
    MaskingCamelLogger(String logName) {
        super(LoggerFactory.getLogger(logName), LoggingLevel.ERROR);
    }

    // Masks only a line that is written: a DEBUG retry line costs nothing while DEBUG is off.
    @Override
    public void log(String message, LoggingLevel loggingLevel) {
        if (shouldLog(getLog(), loggingLevel)) {
            super.log(ErrorTextMasking.mask(message), loggingLevel);
        }
    }

    @Override
    public void log(String message) {
        super.log(ErrorTextMasking.mask(message));
    }

    @Override
    public void doLog(String message) {
        super.doLog(ErrorTextMasking.mask(message));
    }

    @Override
    public void log(String message, Throwable exception, LoggingLevel loggingLevel) {
        if (shouldLog(getLog(), loggingLevel)) {
            super.log(ErrorTextMasking.mask(message), ErrorTextMasking.forLog(exception), loggingLevel);
        }
    }

    @Override
    public void log(String message, Throwable exception) {
        super.log(ErrorTextMasking.mask(message), ErrorTextMasking.forLog(exception));
    }
}
