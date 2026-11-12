package io.openkache.core.replication.ionia;

/** Stable view metadata, independent of the optional per-operation WAL+fsync extension.
 * Production save must survive restart before returning. Called on the control owner, never I/O threads.
 */
public interface IoniaViewStore {
    void save(long view);
    static IoniaViewStore simulation() { return view -> { }; }
}
