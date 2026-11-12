package io.openkache.core.migration.strategy;

import java.util.Properties;

/** B3 전용 연구 설정. 시간은 논리 window, 크기는 bytes이며 생성 후 불변이다. */
public record ScalingConfig(int windows, int keys, int initialNodes, int minNodes, int maxNodes,
        long keyBytes, long nodeBytes, long transferBytes, int provisionWindows, int cooldownWindows,
        int controlWindows, double readService, double writeService, double slo, double low, double high,
        double target, double migrationPenalty, int period, int trainingPeriods, int forecastHorizon,
        double writeRatio, long placementSearchLimit) {
    public ScalingConfig {
        if (windows < 1 || keys < 4 || keys % 4 != 0 || minNodes < 1 || initialNodes < minNodes
                || maxNodes < initialNodes || keyBytes < 1 || nodeBytes < keyBytes || transferBytes < 1
                || provisionWindows < 0 || cooldownWindows < 0 || controlWindows < 1 || period < 4
                || trainingPeriods < 3 || forecastHorizon < 1 || forecastHorizon >= period || placementSearchLimit < 1
                || !Double.isFinite(readService) || readService <= 0 || !Double.isFinite(writeService) || writeService <= 0
                || !Double.isFinite(slo) || slo <= 0 || !(0 < low && low < target && target < high && high <= 1)
                || !(migrationPenalty >= 0 && migrationPenalty < 1) || !(writeRatio >= 0 && writeRatio <= 1))
            throw new IllegalArgumentException("invalid B3 scaling configuration");
    }
    public static ScalingConfig defaults() { return from(new Properties()); }
    /** scaling.* 속성만 읽는다. 외부 서비스나 파일에는 접근하지 않는다. */
    public static ScalingConfig from(Properties p) {
        return new ScalingConfig(i(p,"windows",96), i(p,"keys",32), i(p,"initial.nodes",4), i(p,"min.nodes",2), i(p,"max.nodes",8),
                l(p,"key.bytes",256),l(p,"node.bytes",8192),l(p,"transfer.bytes",2048),i(p,"provision.windows",2),
                i(p,"cooldown.windows",2),i(p,"control.windows",4),d(p,"read.service",.01),d(p,"write.service",.02),
                d(p,"slo",.12),d(p,"low",.35),d(p,"high",.75),d(p,"target",.60),d(p,"migration.penalty",.15),
                i(p,"period",48),i(p,"training.periods",6),i(p,"forecast.horizon",24),d(p,"write.ratio",.10),l(p,"placement.search.limit",2_000_000));
    }
    public ScalingConfig profile(boolean quick) {
        return new ScalingConfig(quick?48:96,keys,initialNodes,minNodes,maxNodes,keyBytes,nodeBytes,transferBytes,
                provisionWindows,cooldownWindows,controlWindows,readService,writeService,slo,low,high,target,
                migrationPenalty,period,trainingPeriods,quick?16:forecastHorizon,writeRatio,placementSearchLimit);
    }
    private static int i(Properties p,String k,int v) { return Integer.parseInt(p.getProperty("scaling."+k,""+v)); }
    private static long l(Properties p,String k,long v) { return Long.parseLong(p.getProperty("scaling."+k,""+v)); }
    private static double d(Properties p,String k,double v) { return Double.parseDouble(p.getProperty("scaling."+k,""+v)); }
}
