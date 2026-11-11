package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.SocketTimeoutException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

class StoreErrorClassTest {
    @ParameterizedTest
    @CsvSource({"22021,RECORD_DATA", "22P05,RECORD_DATA", "22001,RECORD_DATA", "54000,RECORD_DATA",
            "25006,READ_ONLY", "42501,PRIVILEGE", "28P01,PRIVILEGE", "42P01,SCHEMA", "42703,SCHEMA", "23505,SCHEMA",
            "3D000,SCHEMA", "3F000,SCHEMA",
            "08006,UNAVAILABLE", "57P01,UNAVAILABLE", "53100,UNAVAILABLE", "55P03,UNAVAILABLE", "57014,UNAVAILABLE",
            "40001,UNAVAILABLE"})
    void classifiesBySqlState(String sqlState, StoreErrorClass expected) {
        var failure = new IllegalStateException("store call failed",
                new RuntimeException("wrapped", new SQLException("from the database", sqlState)));
        assertEquals(expected, StoreErrorClass.of(failure));
        assertEquals(" storeError=" + expected.label() + " sqlState=" + sqlState, StoreErrorClass.logSuffix(failure));
    }

    @Test
    void aStepOutputThatCouldNotBeCapturedHasItsOwnClass() {
        var failure = new RuntimeException(new no.basis.integrationplatform.camel.step.StepOutputCheckpointException(
                "Output-producing step 'create-order' exceeded the inline checkpoint threshold"));
        assertEquals(StoreErrorClass.CHECKPOINT, StoreErrorClass.of(failure));
        assertEquals(" storeError=checkpoint", StoreErrorClass.logSuffix(failure));
    }

    @Test
    void aCommitReportedWithAWarningIsUnavailableWhateverTheWarningsSqlState() {
        // A trigger could raise a warning with a data SQLSTATE; the record is still read again.
        var failure = assertThrows(SQLException.class, () -> MessageStoreTimeouts.requireConfirmedCommit(
                new java.sql.SQLWarning("raised by a deferred trigger", "22023")));
        assertEquals("08007", failure.getSQLState());
        assertEquals(StoreErrorClass.UNAVAILABLE, StoreErrorClass.of(new RuntimeException(failure)));
    }

    @Test
    void theOutermostSqlStateWins() {
        var inner = new SQLException("index row too large", "54000");
        var outer = new SQLException("cannot execute INSERT in a read-only transaction", "25006", inner);
        assertEquals(StoreErrorClass.READ_ONLY, StoreErrorClass.of(new RuntimeException(outer)));
    }

    @Test
    void withoutASqlStateTheStoreIsUnavailable() {
        var failure = new RuntimeException(new SQLException("socket", new SocketTimeoutException("Read timed out")));
        assertEquals(StoreErrorClass.UNAVAILABLE, StoreErrorClass.of(failure));
        assertNull(StoreErrorClass.sqlState(failure));
        assertEquals(" storeError=unavailable", StoreErrorClass.logSuffix(failure));
        assertEquals(StoreErrorClass.UNAVAILABLE, StoreErrorClass.of(null));
    }
}
