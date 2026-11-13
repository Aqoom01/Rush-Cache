package io.openkache.server.integration;

import io.openkache.core.failure.MembershipListener;
import java.io.IOException;

/** Group-local detector lifecycle. Membership notifications are failure signals, never promotion grants. */
interface FailureRuntime extends AutoCloseable {
    String json();
    @Override void close();
    static FailureRuntime start(IntegrationConfig config, int group, int replica, MembershipListener listener, JsonLine journal) throws IOException {
        return FailureMethods.legacy(config.failureMethod()) ? new FailureBridge(config, group, replica, listener, journal)
                : new LabFailureRuntime(config, group, replica, listener, journal);
    }
}
