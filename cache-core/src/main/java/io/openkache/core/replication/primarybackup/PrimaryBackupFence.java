package io.openkache.core.replication.primarybackup;

import io.openkache.core.api.Deadline;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.Objects;

/** Fixed 3-node, one-successor fencing. Every method runs on the PB owner. */
public final class PrimaryBackupFence {
    public enum Kind { ACQUIRE, GRANT, REQUEST_VOTE, VOTE }

    /** Only votes carry the frozen, applied backup history digest. Rounds are scoped to an epoch. */
    public record Message(OwnershipEpoch epoch, Kind kind, long round, long durationNanos, Bytes digest) implements PeerMessage {
        public Message {
            Objects.requireNonNull(epoch);
            Objects.requireNonNull(kind);
            Objects.requireNonNull(digest);
            boolean lease = kind == Kind.ACQUIRE || kind == Kind.GRANT;
            if ((lease ? round <= 0 || durationNanos <= 0 : round != 0 || durationNanos != 0)
                    || digest.size() != (kind == Kind.VOTE ? 32 : 0)) {
                throw new IllegalArgumentException("invalid PB fencing message");
            }
        }
    }

    private final NodeId self, original, candidate, survivor;
    private final OwnershipEpoch first, next;
    private final long leaseNanos, grantNanos, renewNanos;
    private final TimeSource time;
    private final Transport transport;
    private boolean dead, voteRequested, voted, originalFenced;
    private long leaseRound, grantRound;
    private Deadline renewAt, offeredUntil, authority, grantUntil;
    private Bytes peerDigest;
    private OwnershipEpoch epoch;

    PrimaryBackupFence(PrimaryBackupConfig config, NodeId candidate, NodeId survivor, TimeSource time,
                       Transport transport, Duration renew, Duration lease) {
        self = config.nodeId(); original = config.primaryId(); first = config.epoch(); next = first.next();
        this.candidate = candidate; this.survivor = survivor; this.time = time; this.transport = transport;
        renewNanos = renew.toNanos(); leaseNanos = lease.toNanos();
        grantNanos = Math.multiplyExact(leaseNanos, 2);
        epoch = first;
    }

    Deadline authority() { return authority; }
    boolean hasAuthority() { return active(authority); }
    boolean voted() { return voted; }
    boolean dead() { return dead; }
    boolean peerVoted() { return peerDigest != null; }
    boolean originalFenced() { return originalFenced; }
    void observeDead(NodeId member) {
        if (self.equals(candidate) && epoch.equals(first) && member.equals(original)) dead = true;
    }

    void advanceEpoch() {
        epoch = next;
        grantRound = 0; leaseRound = 0;
        grantUntil = null; authority = null; offeredUntil = null; renewAt = null;
    }

    boolean tick(boolean ready, boolean drained, Bytes digest) {
        long now = time.monotonicNanos();
        fenceOriginalOnExpiry();
        if (ready && !originalFenced && self.equals(primary()) && !active(renewAt)) {
            renewAt = new Deadline(now + renewNanos);
            offeredUntil = new Deadline(now + leaseNanos);
            leaseRound++;
            send(survivor, epoch, Kind.ACQUIRE, leaseRound, Bytes.empty());
            if (epoch.equals(first)) send(candidate, epoch, Kind.ACQUIRE, leaseRound, Bytes.empty());
        }
        if (!epoch.equals(first)) return false;
        if ((dead || voteRequested) && !active(grantUntil)) voted = true;
        if (self.equals(candidate) && voted) {
            if (drained && digest != null && digest.equals(peerDigest)) return true;
            if (!active(renewAt)) {
                renewAt = new Deadline(now + renewNanos);
                send(survivor, next, Kind.REQUEST_VOTE, 0, Bytes.empty());
            }
        } else if (self.equals(survivor) && voted && drained && digest != null && !active(renewAt)) {
            renewAt = new Deadline(now + renewNanos);
            send(candidate, next, Kind.VOTE, 0, digest);
        }
        return false;
    }

    void receive(NodeId from, Message message) {
        if (message.durationNanos() != 0 && message.durationNanos() != leaseNanos) return;
        switch (message.kind()) {
            case ACQUIRE -> grant(from, message);
            case GRANT -> {
                fenceOriginalOnExpiry();
                if (!originalFenced && self.equals(primary()) && message.epoch().equals(epoch) && eligibleGrantor(from)
                        && message.round() == leaseRound && active(offeredUntil)) authority = offeredUntil;
            }
            case REQUEST_VOTE -> {
                if (self.equals(survivor) && from.equals(candidate) && epoch.equals(first)
                        && message.epoch().equals(next)) voteRequested = true;
            }
            case VOTE -> {
                if (self.equals(candidate) && from.equals(survivor) && voted && epoch.equals(first)
                        && message.epoch().equals(next)) peerDigest = message.digest();
            }
        }
    }

    private void fenceOriginalOnExpiry() {
        if (self.equals(original) && authority != null && !active(authority)) originalFenced = true;
    }

    private void grant(NodeId from, Message message) {
        if (!message.epoch().equals(epoch) || !from.equals(primary()) || !eligibleGrantor(self)
                || (epoch.equals(first) && (dead || voteRequested || voted)) || message.round() < grantRound) return;
        if (message.round() > grantRound) {
            grantRound = message.round();
            grantUntil = new Deadline(time.monotonicNanos() + grantNanos);
        }
        if (active(grantUntil)) send(from, epoch, Kind.GRANT, message.round(), Bytes.empty());
    }

    private NodeId primary() { return epoch.equals(first) ? original : candidate; }
    private boolean eligibleGrantor(NodeId node) { return node.equals(survivor) || (epoch.equals(first) && node.equals(candidate)); }
    private boolean active(Deadline deadline) { return deadline != null && !deadline.isExpired(time); }
    private void send(NodeId to, OwnershipEpoch messageEpoch, Kind kind, long round, Bytes digest) {
        transport.send(new PeerEnvelope(self, to, new Message(messageEpoch, kind, round, round == 0 ? 0 : leaseNanos, digest)));
    }
}
