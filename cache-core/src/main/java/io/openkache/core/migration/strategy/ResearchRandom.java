package io.openkache.core.migration.strategy;

import java.util.SplittableRandom;

/** 명시적 Java generator 버전의 연구 입력. NumPy PCG64와 같은 seed 출력이라고 주장하지 않는다. */
public final class ResearchRandom {
    private ResearchRandom(){}
    public static int poisson(double lambda,SplittableRandom random){
        if(!Double.isFinite(lambda)||lambda<0||lambda>1_000_000)throw new IllegalArgumentException("invalid rate");int count=0;
        while(lambda>0){double chunk=Math.min(20,lambda),threshold=Math.exp(-chunk),product=1;int k=0;do{k++;product*=random.nextDouble();}while(product>threshold);count+=k-1;lambda-=chunk;}return count;
    }
    public static double quantile(double[] values,double q){if(q<0||q>1)throw new IllegalArgumentException("invalid quantile");return Numerics.quantile(values,q);}
}
