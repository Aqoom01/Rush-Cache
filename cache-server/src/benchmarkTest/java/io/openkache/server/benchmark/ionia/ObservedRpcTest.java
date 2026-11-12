package io.openkache.server.benchmark.ionia;

import static io.openkache.server.benchmark.ionia.AdapterFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaClientTransport;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class ObservedRpcTest {
    private IoniaClientTransport.Request request() {
        return new IoniaClientTransport.Request(0, delete(), new ClientId(7), new OperationId(1),
                RoutingVersion.ZERO, OwnershipEpoch.ZERO, 1000);
    }
    @Test void cancellationOfPublicCopyDoesNotFreeTheRawPermit() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        ObservedRpc rpc = new ObservedRpc(raw, 1);
        var stage = rpc.execute(IDS.getFirst(), request());
        stage.toCompletableFuture().cancel(true);
        assertEquals(1, rpc.snapshot().outstanding());
        assertThrows(ExecutionException.class, () -> rpc.execute(IDS.getFirst(), request()).toCompletableFuture().get());
        rpc.stopAdmission();
        assertEquals(1, rpc.snapshot().outstanding());
        held.complete(result(ResultStatus.OK));
        assertEquals(0, rpc.snapshot().outstanding());
        assertEquals(1, rpc.snapshot().byKind().get(ObservedRpc.Kind.EXECUTE).completed());
    }
    @Test void synchronousTransportThrowReleasesPermitAndRecordsFailure() {
        Fake raw = new Fake(); raw.execute = r -> { throw new IllegalStateException("fixture"); };
        ObservedRpc rpc = new ObservedRpc(raw, 1);
        assertThrows(ExecutionException.class, () -> rpc.execute(IDS.getFirst(), request()).toCompletableFuture().get());
        assertEquals(0, rpc.snapshot().outstanding());
        assertEquals(1, rpc.snapshot().byKind().get(ObservedRpc.Kind.EXECUTE).failed());
    }
    @Test void exceptionalRawCompletionReleasesPermitOnce() {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        ObservedRpc rpc = new ObservedRpc(raw, 1);
        rpc.execute(IDS.getFirst(), request());
        held.completeExceptionally(new TimeoutException("fixture"));
        assertFalse(held.complete(result(ResultStatus.OK)));
        assertEquals(0, rpc.snapshot().outstanding());
        assertEquals(1, rpc.snapshot().byKind().get(ObservedRpc.Kind.EXECUTE).completed());
        assertEquals(1, rpc.snapshot().byKind().get(ObservedRpc.Kind.EXECUTE).failed());
    }
}

