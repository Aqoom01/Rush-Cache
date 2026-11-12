package io.openkache.core.replication.ionia;

import io.openkache.core.api.CacheEngine;
import java.util.concurrent.CompletionStage;

/** Storage with an exclusive reset boundary for authoritative consensus-log replay.
 * Engine waits for all old applies before reset and refuses normal service until replay finishes.
 */
public interface IoniaStorage extends CacheEngine {
    CompletionStage<Void> reset();
}
