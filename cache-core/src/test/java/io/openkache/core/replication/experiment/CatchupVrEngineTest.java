package io.openkache.core.replication.experiment;

/** INCREMENTAL과 같은 안전성·분단·유실·중복·순서 역전 검사를 INCREMENTAL_CATCHUP에도 적용한다. */
class CatchupVrEngineTest extends IncrementalVrEngineTest {
    @Override protected ProtocolCluster cluster(long seed) { return new ProtocolCluster(ProtocolCluster.Variant.VR_CATCHUP,1,100,seed); }
}
