package io.openkache.runner;

import io.openkache.core.migration.MigrationScheduler.JobView;
import io.openkache.core.migration.MigrationScheduler.Settings;
import io.openkache.core.migration.MigrationScheduler.Status;
import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

final class MigrationAdmission {
    static final long POLL_MS = 5;
    private static final int MAX_RETRIES = 100;

    @FunctionalInterface
    interface Peer {
        WireClient.Response request(long node, int opcode, byte[] body) throws IOException;
    }

    static final class Job {
        final ClusterConfig.Migration plan;
        final Change change;
        final long bytes;
        final Job predecessor;
        Status state = Status.QUEUED;
        double queued = -1;
        double sent = -1;
        double finished = -1;
        long startedTick = -1;
        long endedTick = -1;
        int accept = -1;
        int phase = -1;
        int retries;
        long retryTick;
        byte[] query;
        String note = "";
        String server = "null";

        Job(ClusterConfig.Migration plan, Change change, long bytes, Job predecessor) {
            this.plan = plan;
            this.change = change;
            this.bytes = bytes;
            this.predecessor = predecessor;
        }

        JobView view(int index) {
            return new JobView(index, change, state, startedTick, endedTick, 0, note);
        }

        boolean pending() {
            return state == Status.QUEUED || state == Status.RUNNING;
        }

        ClusterRunner.MigrationRecord record() {
            return new ClusterRunner.MigrationRecord(plan, change.from().value(), sent, accept, note,
                    finished, phase, server, queued, retries);
        }
    }

    private final List<Job> jobs;
    private final Settings settings;
    private final SchedulerAlgorithms policy;
    private final Peer peer;
    private final DoubleSupplier clock;
    final List<Long> admissionOrder = new ArrayList<>();
    int maxObservedConcurrent;

    MigrationAdmission(ClusterConfig.Scheduler config, List<Job> jobs, long rate, Peer peer, DoubleSupplier clock) {
        this.jobs = jobs;
        this.peer = peer;
        this.clock = clock;
        // Uncapped transfers use the full simulator's reference budget for ranking only.
        long nodeBudget = rate == 0 ? 4096 : Math.max(1, rate / (1000 / POLL_MS));
        this.settings = new Settings(config.maxConcurrent(), config.maxPerNode(), nodeBudget * 4,
                nodeBudget, ClusterRunner.MIGRATION_WAIT_MS / POLL_MS);
        List<SchedulerAlgorithms.Task> tasks = new ArrayList<>();
        long deadlineTicks = Math.ceilDiv(config.deadlineMs(), POLL_MS);
        for (int i = 0; i < jobs.size(); i++) {
            Job job = jobs.get(i);
            tasks.add(new SchedulerAlgorithms.Task(i, job.change, job.bytes, 0, deadlineTicks));
        }
        this.policy = new SchedulerAlgorithms(config.policy(), tasks, settings, i -> jobs.get(i).bytes, i -> 0);
    }

    // SchedulerAlgorithms uses logical rounds. One round is one polling pass, nominally 5 ms plus I/O.
    boolean step(long tick) {
        for (Job job : jobs) {
            if (job.state == Status.RUNNING && !poll(tick, job)) return false;
        }
        Map<NodeId, Integer> activeByNode = new HashMap<>();
        List<JobView> views = new ArrayList<>();
        int active = 0;
        double now = clock.getAsDouble();
        for (int i = 0; i < jobs.size(); i++) {
            Job job = jobs.get(i);
            if (job.state == Status.RUNNING) {
                count(activeByNode, job.change);
                active++;
            }
            if (job.state == Status.QUEUED) {
                if (now < job.plan.atMs()) continue;
                if (job.queued < 0) job.queued = now;
                if (job.predecessor != null && job.predecessor.state != Status.DONE) {
                    if (!job.predecessor.pending()) finish(tick, job, Status.FAILED, "previous tablet move did not complete");
                    continue;
                }
                if (tick < job.retryTick) continue;
            }
            views.add(job.view(i));
        }
        for (int index : policy.order(tick, views, settings)) {
            if (active >= settings.maxConcurrentJobs()) break;
            Job job = jobs.get(index);
            if (activeByNode.getOrDefault(job.change.from(), 0) >= settings.maxJobsPerNode()
                    || activeByNode.getOrDefault(job.change.to(), 0) >= settings.maxJobsPerNode()) continue;
            if (!start(tick, index, job)) return false;
            if (job.state == Status.RUNNING) {
                count(activeByNode, job.change);
                maxObservedConcurrent = Math.max(maxObservedConcurrent, ++active);
            }
        }
        return jobs.stream().anyMatch(Job::pending);
    }

    private static void count(Map<NodeId, Integer> active, Change change) {
        active.merge(change.from(), 1, Integer::sum);
        active.merge(change.to(), 1, Integer::sum);
    }

    private boolean start(long tick, int index, Job job) {
        job.sent = clock.getAsDouble();
        WireClient.Response response;
        try {
            response = peer.request(job.change.from().value(), ClusterRunner.OP_MIGRATE,
                    ClusterRunner.migrationBody(job.plan));
        } catch (IOException e) {
            // The server may have started a job before the response was lost. Stop admissions.
            finish(tick, job, Status.FAILED, "start outcome unknown: " + e);
            return false;
        }
        job.accept = response.status();
        if (job.accept != WireProtocol.STATUS_OK) {
            job.note = response.detail();
            boolean retryable = job.accept == WireProtocol.STATUS_UNAVAILABLE || job.accept == WireProtocol.STATUS_BUSY
                    || (job.accept == WireProtocol.STATUS_BAD_REQUEST && job.note.startsWith("tablet owned by"));
            if (retryable && job.retries < MAX_RETRIES) {
                job.retries++;
                job.retryTick = tick + 1;
                System.out.printf("migration tablet=%d admission retry %d: status=%d %s%n",
                        job.plan.tablet(), job.retries, job.accept, job.note);
            } else {
                finish(tick, job, Status.FAILED, job.note);
            }
            return true;
        }
        if (response.value().length != Long.BYTES) {
            finish(tick, job, Status.FAILED, "invalid migration job ID");
            return false;
        }
        job.query = response.value();
        job.note = "";
        job.startedTick = tick;
        job.state = Status.RUNNING;
        policy.started(tick, job.view(index));
        admissionOrder.add(job.plan.tablet());
        return true;
    }

    private boolean poll(long tick, Job job) {
        try {
            WireClient.Response response = peer.request(job.change.from().value(), ClusterRunner.OP_MIGRATE_STATUS,
                    job.query);
            if (response.status() == WireProtocol.STATUS_OK && response.value().length == 1) {
                job.phase = response.value()[0];
                job.server = response.detail().startsWith("{") ? response.detail() : "null";
                if (job.phase == ClusterRunner.PHASE_DONE) {
                    finish(tick, job, Status.DONE, "");
                    return true;
                }
                if (job.phase == ClusterRunner.PHASE_FAILED) {
                    finish(tick, job, Status.FAILED, "not done");
                    return true;
                }
            } else {
                job.note = "status query rejected: " + response.detail();
            }
        } catch (IOException e) {
            // Keep the slot occupied until a terminal phase is known.
            job.note = "status query failed: " + e;
        }
        if (clock.getAsDouble() - job.sent >= ClusterRunner.MIGRATION_WAIT_MS) {
            finish(tick, job, Status.TIMED_OUT, "status timeout " + job.note);
            return false;
        }
        return true;
    }

    void stop(long tick, String reason) {
        for (Job job : jobs) {
            if (job.pending()) finish(tick, job, Status.TIMED_OUT, reason);
        }
    }

    private void finish(long tick, Job job, Status status, String note) {
        job.state = status;
        job.endedTick = tick;
        job.finished = clock.getAsDouble();
        job.note = note;
    }
}
