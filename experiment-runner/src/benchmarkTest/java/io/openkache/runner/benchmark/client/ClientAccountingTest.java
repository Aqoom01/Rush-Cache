package io.openkache.runner.benchmark.client;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ClientAccountingTest {
    @Test void conservationAndPerOperationCountersDistinguishAttemptsFromAdmission() {
        ClientAccounting ledger = new ClientAccounting();
        ledger.admitted(ClientOutcome.Operation.GET);
        ledger.admitted(ClientOutcome.Operation.SET);
        ledger.admitted(ClientOutcome.Operation.DELETE);
        ledger.admitted(ClientOutcome.Operation.GET);
        ledger.attempt(ClientOutcome.Operation.GET); ledger.attempt(ClientOutcome.Operation.GET);
        ledger.discovery(ClientOutcome.Operation.GET);
        ledger.terminal(ClientOutcome.Operation.GET, ClientOutcome.Category.SUCCESS);
        ledger.terminal(ClientOutcome.Operation.SET, ClientOutcome.Category.ERROR_RESPONSE);
        ledger.terminal(ClientOutcome.Operation.DELETE, ClientOutcome.Category.NO_RESPONSE);
        ledger.rejected(); ledger.duplicateSubmission();
        var s = ledger.snapshot();
        assertEquals(4, s.admitted());
        assertEquals(1, s.success()); assertEquals(1, s.errorResponse());
        assertEquals(1, s.noResponse()); assertEquals(1, s.pending());
        assertEquals(s.admitted(), s.success() + s.errorResponse() + s.noResponse() + s.pending());
        assertEquals(2, s.attempts());
        assertEquals(1, s.rejected()); assertEquals(1, s.duplicateSubmissions());
        assertThrows(UnsupportedOperationException.class, () -> s.byOperation().clear());
    }
    @Test void duplicateTerminalAndTerminalWithoutAdmissionAreDetected() {
        ClientAccounting ledger = new ClientAccounting();
        assertThrows(IllegalStateException.class, () -> ledger.terminal(ClientOutcome.Operation.GET, ClientOutcome.Category.SUCCESS));
        ledger.admitted(ClientOutcome.Operation.GET);
        ledger.terminal(ClientOutcome.Operation.GET, ClientOutcome.Category.NO_RESPONSE);
        assertThrows(IllegalStateException.class, () -> ledger.terminal(ClientOutcome.Operation.GET, ClientOutcome.Category.SUCCESS));
        assertEquals(1, ledger.snapshot().noResponse());
    }
}

