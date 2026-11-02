package no.basis.integrationplatform.camel.step;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** How one attempt's record tells a lost part's skip from a normal one, and how it stays bounded. */
class StepVisitsTest {
    @Test
    void aSkipIsARepeatOnlyWhenAnotherPartClaimedTheKeyFirst() {
        StepVisits visits = new StepVisits();
        assertNull(visits.claim("/split.1#0", "m:send", false));
        visits.delivering("/split.1#0", "m:send");
        assertEquals(StepVisits.SPLIT, visits.claim("/split.1#1", "m:send", true));
        assertNull(visits.claim("/split.1#0", "m:send", true), "the part that sent the key skips its own send");
        // An earlier attempt's delivery: the first part to skip it owns the skip.
        assertNull(visits.claim("/split.1#0", "m:other", true));
        assertEquals(StepVisits.SPLIT, visits.claim("/split.1#1", "m:other", true));
    }

    @Test
    void theScopeNamesTheKindWhereTwoCallsOfOneSplitDiffer() {
        StepVisits visits = new StepVisits();
        visits.claim("/call.1#/split.3#0", "m:send", false);
        visits.delivering("/call.1#/split.3#0", "m:send");
        assertEquals(StepVisits.SPLIT, visits.claim("/call.2#/split.3#0", "m:send", true));
        assertEquals(StepVisits.LOOP, visits.claim("/loop.4#1", "m:send", true));
    }

    @Test
    void aKeyThatEveryPartReachesStaysWhileOthersAreForgottenBeyondTheLimit() {
        StepVisits visits = new StepVisits();
        visits.claim("/split.1#0", "m:shared", false);
        visits.delivering("/split.1#0", "m:shared");
        visits.claim("/split.1#0", "m:first-own", false);
        for (int i = 1; i <= StepVisits.MAX_KEYS; i++) {
            assertEquals(StepVisits.SPLIT, visits.claim("/split.1#" + i, "m:shared", true));
            visits.claim("/split.1#" + i, "m:own-" + i, false);
        }
        // The shared key is still known; the oldest per-part key was forgotten, so a skip
        // of it by another part now looks like an earlier attempt's delivery.
        assertEquals(StepVisits.SPLIT, visits.claim("/split.1#x", "m:shared", true));
        assertNull(visits.claim("/split.1#y", "m:first-own", true));
    }
}
