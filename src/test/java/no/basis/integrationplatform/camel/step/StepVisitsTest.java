package no.basis.integrationplatform.camel.step;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** How one attempt's record tells a lost part's skip from a normal one, and how it stays bounded. */
class StepVisitsTest {
    @Test
    void aSkipIsARepeatOnlyWhenAPartAtAnotherPositionSentTheKeyInThisAttempt() {
        StepVisits visits = new StepVisits();
        assertNull(visits.repeatedIn("/split.1#0", "m:send"), "an earlier attempt's delivery");
        assertNull(visits.repeatedIn("/split.1#1", "m:send"), "an earlier attempt's delivery");
        visits.sent("/split.1#0", "m:line");
        assertEquals(StepVisits.SPLIT, visits.repeatedIn("/split.1#1", "m:line"));
        assertNull(visits.repeatedIn("/split.1#0", "m:line"), "the part that sent the key skips its own send");
        assertEquals(StepVisits.LOOP, visits.repeatedIn("/split.1#0/loop.2#1", "m:line"), "a loop inside the sending part");
        assertNull(visits.repeatedIn("", "m:line"), "the input itself, outside any part");
        visits.sent("/split.1#2/multicast.3#0", "m:branch");
        assertNull(visits.repeatedIn("/split.1#2", "m:branch"), "one of this part's own branches sent it");
        assertEquals(StepVisits.MULTICAST, visits.repeatedIn("/split.1#2/multicast.3#1", "m:branch"));
    }

    @Test
    void partsThatSentInParallelEachSkipTheirOwnSend() {
        StepVisits visits = new StepVisits();
        visits.sent("/split.1#0", "m:send");
        visits.sent("/split.1#1", "m:send");
        assertNull(visits.repeatedIn("/split.1#0", "m:send"));
        assertNull(visits.repeatedIn("/split.1#1", "m:send"));
        assertEquals(StepVisits.SPLIT, visits.repeatedIn("/split.1#2", "m:send"));
    }

    @Test
    void anUnknownLoopIterationIsNeverARepeat() {
        StepVisits visits = new StepVisits();
        visits.sent("/loop.2#0", "m:send");
        assertNull(visits.repeatedIn("/loop.2#?", "m:send"));
        visits.sent("/loop.2#?", "m:other");
        assertNull(visits.repeatedIn("/loop.2#1", "m:other"), "a key sent from an unknown iteration is not checked");
    }

    @Test
    void forgottenKeysAndKeysSentByManyPartsAreNotChecked() {
        StepVisits visits = new StepVisits();
        visits.sent("/split.1#0", "m:old");
        for (int i = 0; i < StepVisits.MAX_KEYS; i++) {
            visits.sent("/split.1#0", "m:own-" + i);
        }
        assertNull(visits.repeatedIn("/split.1#1", "m:old"));

        StepVisits crowded = new StepVisits();
        for (int i = 0; i <= StepVisits.MAX_SENDERS; i++) {
            crowded.sent("/split.1#" + i, "m:send");
        }
        assertNull(crowded.repeatedIn("/split.1#" + (StepVisits.MAX_SENDERS + 1), "m:send"));
    }
}
