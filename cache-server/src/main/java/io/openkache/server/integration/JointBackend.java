package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessageCodec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A1 is the replicated admission/metadata authority; A2 is the SSD data plane.
 * The single gateway first commits a request fingerprint through A1, then sends
 * the command to A2. A1 never receives user values. Its native failover really
 * blocks/resumes new admissions. A2 owns data ordering, Fast ACK and view recovery.
 * The lock covers admission only: A2 requests may overlap/batch after admission.
 * This composition does not claim to reproduce a new combined consensus protocol.
 */
final class JointBackend implements ReplicatedBackend {
    static final Bytes ADMISSION = Bytes.utf8("openkache/control/admission");
    static final Bytes ROUTING = Bytes.utf8("openkache/control/routing");
    private final IntegrationConfig config;
    private final ReplicatedBackend authority, data;
    private final ReentrantLock admission = new ReentrantLock(true);
    private final AtomicLong tickets = new AtomicLong(), adminIds = new AtomicLong();
    private final LongAdder admitted = new LongAdder(), denied = new LongAdder(), dataCalls = new LongAdder(), publications = new LongAdder();
    JointBackend(IntegrationConfig config, int group) {
        this(config, new ReplicatedBackend.Native(config.layer(true), group), new ReplicatedBackend.Native(config.layer(false), group));
    }
    JointBackend(IntegrationConfig config, ReplicatedBackend authority, ReplicatedBackend data) {
        if (!config.composed()) throw new IllegalArgumentException("joint A1/A2 configuration required");
        this.config = config; this.authority = authority; this.data = data;
    }
    @Override public boolean supportsDelete() { return data.supportsDelete(); }
    @Override public Result execute(Command command, long client, long operation, long deadline) throws IOException, InterruptedException {
        if (!admission.tryLock(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) { denied.increment(); return ReplicatedBackend.status(ResultStatus.BUSY); }
        try {
            if (System.nanoTime() >= deadline) { denied.increment(); return ReplicatedBackend.status(ResultStatus.BUSY); }
            Bytes receipt = fingerprint(command, client, operation, tickets.incrementAndGet());
            try {
                if (!storeAndConfirm(ADMISSION, receipt, deadline)) { denied.increment(); return ReplicatedBackend.status(ResultStatus.BUSY); }
            } catch (IOException failure) {
                // No data command was submitted. A possibly committed control receipt is harmless.
                denied.increment(); return ReplicatedBackend.status(ResultStatus.BUSY);
            }
            admitted.increment();
        } finally { admission.unlock(); }
        if (System.nanoTime() >= deadline) { denied.increment(); return ReplicatedBackend.status(ResultStatus.BUSY); }
        dataCalls.increment();
        Result result = data.execute(command, client, operation, deadline);
        // Native IONIA write BUSY can follow recording. Do not expose it as a safe admission retry.
        return !(command instanceof Command.Get) && result.status() == ResultStatus.BUSY ? ReplicatedBackend.status(ResultStatus.TIMEOUT) : result;
    }
    private boolean storeAndConfirm(Bytes key, Bytes value, long deadline) throws IOException, InterruptedException {
        Result written = authority.administrative(new Command.Set(key, value), deadline);
        if (written.status() != ResultStatus.OK || System.nanoTime() >= deadline) return false;
        Result read = authority.administrative(new Command.Get(key), deadline);
        return read.status() == ResultStatus.OK && read.value().filter(value::equals).isPresent() && System.nanoTime() < deadline;
    }
    @Override public Result administrative(Command command) throws IOException, InterruptedException {
        long operation = adminIds.incrementAndGet();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.number("request.timeout.ms", 3000));
        Result result;
        do {
            result = execute(command, 1048574, operation, deadline);
            // A1 may still be electing/repairing. BUSY means no data write was
            // submitted; preserve the operation ID and total deadline while waiting.
            // A2's ambiguous write BUSY was converted to TIMEOUT above, so is never replayed here.
            if (result.status() == ResultStatus.BUSY && System.nanoTime() < deadline) Thread.sleep(5);
            else return result;
        } while (System.nanoTime() < deadline);
        return result;
    }
    @Override public void publishRouting(RoutingTable before, RoutingTable after) throws IOException, InterruptedException {
        admission.lockInterruptibly();
        try {
            Result current = authority.administrative(new Command.Get(ROUTING));
            Bytes next = Bytes.copyOf(TableMessageCodec.encodeTable(after));
            // A retried publication may already be committed. Never overwrite a different predecessor.
            if (current.status() == ResultStatus.OK && current.value().filter(next::equals).isPresent()) return;
            boolean expected = before == null ? current.status() == ResultStatus.MISS
                    : current.status() == ResultStatus.OK && current.value().filter(Bytes.copyOf(TableMessageCodec.encodeTable(before))::equals).isPresent();
            if (!expected) throw new IOException("A1 routing predecessor mismatch/unavailable");
            long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.number("request.timeout.ms", 3000));
            if (!storeAndConfirm(ROUTING, next, until)) throw new IOException("A1 routing publication not confirmed");
            publications.increment();
        } finally { admission.unlock(); }
    }
    private static Bytes fingerprint(Command command, long client, long operation, long ticket) {
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            hash.update(ByteBuffer.allocate(24).putLong(client).putLong(operation).putLong(ticket).array());
            Bytes key = command instanceof Command.Get g ? g.key() : command instanceof Command.Set s ? s.key() : ((Command.Delete) command).key();
            hash.update((byte) (command instanceof Command.Get ? 0 : command instanceof Command.Set ? 1 : 2));
            hash.update(ByteBuffer.allocate(4).putInt(key.size()).array()); hash.update(key.toByteArray());
            if (command instanceof Command.Set s) hash.update(s.value().toByteArray());
            return Bytes.copyOf(hash.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    @Override public OptionalDouble cpu() { return data.cpu(); } // Both layers share each replica JVM: process CPU includes A1.
    @Override public void confirmedCrash(int replica) throws IOException, InterruptedException { authority.confirmedCrash(replica); data.confirmedCrash(replica); }
    @Override public String compositionStatus() {
        return "{\"a1\":" + JsonLine.quote(config.a1Method()) + ",\"a2\":" + JsonLine.quote(config.a2Method())
                + ",\"authorityAdmissions\":" + admitted.sum() + ",\"authorityDenied\":" + denied.sum()
                + ",\"dataCalls\":" + dataCalls.sum() + ",\"routingPublications\":" + publications.sum() + "}";
    }
    @Override public void close() { try { data.close(); } finally { authority.close(); } }
}
