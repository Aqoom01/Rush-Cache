package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/** 독립 calibration으로 학습한 get/put 성능 경계. 실측 지연으로 오인하지 않는 연구 모델이다. */
public final class PerformanceModel {
    private final ScalingConfig config;
    private final List<double[]> rows,rewards;
    private final double[] coefficients;
    public PerformanceModel(ScalingConfig config,List<double[]> observations,List<double[]> rewardRows){
        this.config=config;this.rows=copy(observations);this.rewards=copy(rewardRows);
        if(rows.size()<2)throw new IllegalArgumentException("calibration data required");
        double[][] x=new double[rows.size()][2];double[] y=new double[rows.size()];boolean positive=false,negative=false;
        for(int i=0;i<rows.size();i++){double[] r=rows.get(i);if(r.length!=4)throw new IllegalArgumentException("invalid calibration row");x[i][0]=r[0];x[i][1]=r[1];y[i]=r[2]>config.slo()?1:0;positive|=y[i]==1;negative|=y[i]==0;}
        if(!positive||!negative)throw new IllegalArgumentException("calibration needs both SLO classes");coefficients=Numerics.logistic(x,y);
    }
    public static PerformanceModel calibrate(ScalingConfig config){
        List<double[]> rows=new ArrayList<>(),rewards=new ArrayList<>();
        for(double ratio:new double[]{0,.1,.5,.9,1})for(int rate=20;rate<=120;rate+=10)for(int seed=0;seed<3;seed++){
            double[] observed=queueSample(rate,ratio,9000+seed,config);rows.add(new double[]{rate*(1-ratio),rate*ratio,observed[0],observed[1]});}
        for(int nodes=config.minNodes();nodes<=config.maxNodes();nodes++)for(int total=40;total<=800;total+=40)for(int seed=0;seed<3;seed++){
            double[] observed=queueSample((double)total/nodes,config.writeRatio(),12000+seed,config);rewards.add(new double[]{nodes,total,observed[0],observed[1]*nodes});}
        return new PerformanceModel(config,rows,rewards);
    }
    public static double[] queueSample(double rate,double ratio,long seed,ScalingConfig c){
        var random=new SplittableRandom(seed);double free=0;long completed=0;var latencies=new ArrayList<Double>();
        for(int t=0;t<6;t++){int count=ResearchRandom.poisson(rate,random);double[] arrivals=new double[count];for(int i=0;i<count;i++)arrivals[i]=t+random.nextDouble();Arrays.sort(arrivals);
            for(double arrival:arrivals){free=Math.max(free,arrival)+(random.nextDouble()<ratio?c.writeService():c.readService());if(t>=3)latencies.add(free-arrival);if(free<=6)completed++;}}
        return new double[]{Numerics.quantile(latencies.stream().mapToDouble(Double::doubleValue).toArray(),.99),completed/6.};
    }
    public ScalingConfig config(){return config;}
    public double capacity(){return limit(config.writeRatio());}
    public double limit(double ratio){return ScalingSupport.clip(-100*coefficients[2]/Math.max(1e-6,coefficients[0]*(1-ratio)+coefficients[1]*ratio),15,100);}
    public boolean fits(double reads,double writes){return coefficients[0]*reads/100+coefficients[1]*writes/100+coefficients[2]<=0;}
    public List<double[]> rewardData(){return copy(rewards);}
    public List<double[]> observations(){return copy(rows);}
    public double[] coefficients(){return coefficients.clone();}
    public Map<String,Object> record(){return Map.of("calibration_rows",rows.size(),"reward_rows",rewards.size(),"capacity",capacity(),"logistic_coefficients",List.of(coefficients[0],coefficients[1]),"logistic_intercept",coefficients[2],"fit","jdk-newton-l2-C1000-v1");}
    private static List<double[]> copy(List<double[]> rows){return rows.stream().map(double[]::clone).toList();}
}
