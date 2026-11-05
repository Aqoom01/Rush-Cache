package io.openkache.core.migration.strategy;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ScalingStrategiesTest {
    static ScalingConfig config;static PerformanceModel model;
    @BeforeAll static void prepare(){config=ScalingConfig.defaults().profile(true);model=PerformanceModel.calibrate(config);}
    static List<Double> past(){var out=new ArrayList<Double>();for(int t=-config.period()*config.trainingPeriods();t<0;t++)out.add(200+80*Math.sin(2*Math.PI*t/config.period()));return out;}
    static ScalingSnapshot snapshot(String method,boolean hot){var cluster=new ScalingSimulation(config,method);double[] r=new double[config.keys()],w=new double[config.keys()];Arrays.fill(r,3);if(hot)Arrays.fill(r,0,4,40);var cpu=new java.util.HashMap<Integer,Double>();cluster.active().forEach(n->cpu.put(n,.5));
        return new ScalingSnapshot(config,3,cluster.active(),cluster.units(),r,w,cpu,.05,Arrays.stream(r).sum(),Arrays.stream(r).sum(),new long[config.maxNodes()],-100,"");}
    @Test void calibrationSeparatesHeldOutLowAndHighLoad(){
        assertTrue(PerformanceModel.queueSample(30,.1,713,config)[0]<config.slo());assertTrue(PerformanceModel.queueSample(130,.1,713,config)[0]>config.slo());assertTrue(model.fits(27,3));assertFalse(model.fits(117,13));
    }
    @Test void sparForecastUsesOnlyPastAndLearnsPeriodicPattern(){var history=past();var predictor=new SparPredictor(history,config);var values=predictor.predict(history);for(int h=0;h<values.size();h++)assertEquals(200+80*Math.sin(2*Math.PI*h/config.period()),values.get(h)/1.1,.002);}
    @Test void dynamicProgramStartsProvisioningBeforePeakAndReportsImpossible(){var predictions=new ArrayList<Double>();for(int i=0;i<16;i++)predictions.add(i<4?80.:260.);var schedule=PStorePolicy.schedule(predictions,2,100,config);assertTrue(Double.isFinite(schedule.cost()));assertTrue(schedule.moves().getFirst().after()>2);assertEquals(0,schedule.moves().getFirst().window());var impossible=PStorePolicy.schedule(java.util.Collections.nCopies(16,1000.),2,100,config);assertTrue(impossible.moves().isEmpty());assertEquals(Double.POSITIVE_INFINITY,impossible.cost());}
    @Test void arimaConstantAndRampAreFinitePastOnlyForecasts(){assertEquals(100,PaxPolicy.forecast(java.util.Collections.nCopies(64,100.),2)[0],1e-8);var ramp=new ArrayList<Double>();for(int i=0;i<64;i++)ramp.add((double)i);assertTrue(PaxPolicy.forecast(ramp,2)[0]>63);}
    @Test void allPoliciesHaveIndependentStateAndPreserveCoverage(){for(String name:ScalingPolicies.METHODS){var a=ScalingPolicies.create(name,model,past());var b=ScalingPolicies.create(name,model,past());var s=snapshot(name,false);a.observe(s.total());b.observe(s.total());var one=a.decide(s);var two=b.decide(s);assertEquals(B3Json.hash(one),B3Json.hash(two),name);ScalingSupport.validate(one,config,false);}}
    @Test void tiramolaActuallyUpdatesQValues(){var policy=new TiramolaPolicy(model);var plan=policy.decide(snapshot("tiramola",false));assertTrue(policy.updates()>0);assertTrue(Math.abs(plan.active().size()-4)<=1);}
    @Test void warmupBlocksScaleInUntilCompletionPlusPreparation(){var base=snapshot("aws-warmup",false);var cpu=new java.util.HashMap<Integer,Double>();base.active().forEach(n->cpu.put(n,.1));var policy=new AwsWarmupPolicy();var during=new ScalingSnapshot(config,5,base.active(),base.units(),base.reads(),base.writes(),cpu,0,1,1,base.hints(),5,"out");assertEquals("warmup-block-scale-in",policy.decide(during).reason());var after=new ScalingSnapshot(config,8,base.active(),base.units(),base.reads(),base.writes(),cpu,0,1,1,base.hints(),5,"out");assertEquals(2,policy.decide(after).active().size());}
    @Test void jsonRecordsAndArraysHaveStableRoundTripEvidence(){var value=Map.of("config",config,"values",new double[]{1,.5});String encoded=B3Json.encode(value);assertEquals(B3Json.hash(value),B3Json.hash(B3Json.decode(encoded)));assertThrows(IllegalArgumentException.class,()->B3Json.decode("{\"a\":1,\"a\":2}"));}
}
