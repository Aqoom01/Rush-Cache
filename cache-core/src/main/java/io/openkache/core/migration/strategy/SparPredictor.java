package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.List;

/** P-Store SPAR(n=2, m=3) horizon별 least-squares. 미래 표본을 받지 않는 불변 학습 모델. */
public final class SparPredictor {
    private final int period,horizon;private final double[][] weights;
    public SparPredictor(List<Double> past,ScalingConfig c){
        period=c.period();horizon=c.forecastHorizon();weights=new double[horizon][];
        if(past.size()<=2*period+5+horizon)throw new IllegalArgumentException("SPAR needs at least three historical periods");
        for(int h=1;h<=horizon;h++){var x=new ArrayList<double[]>();var y=new ArrayList<Double>();for(int t=2*period+4;t<past.size()-h;t++){x.add(features(past.subList(0,t+1),h));y.add(past.get(t+h));}weights[h-1]=Numerics.leastSquares(x.toArray(double[][]::new),y.stream().mapToDouble(Double::doubleValue).toArray());}
    }
    private double[] features(List<Double> history,int h){int t=history.size()-1;double[] values=new double[5];for(int k=1;k<=2;k++)values[k-1]=history.get(t+h-k*period);for(int j=0;j<3;j++)values[j+2]=history.get(t-j)-(history.get(t-j-period)+history.get(t-j-2*period))/2;return values;}
    public List<Double> predict(List<Double> history){var result=new ArrayList<Double>();for(int h=1;h<=horizon;h++)result.add(Math.max(0,Numerics.dot(weights[h-1],features(history,h)))*1.1);return List.copyOf(result);}
}
