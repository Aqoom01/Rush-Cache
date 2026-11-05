package io.openkache.core.migration.strategy;

import java.util.Arrays;

/** 작은 B3 회귀 문제용 JDK 수치 계산. 전체 범용 최적화 라이브러리가 아니다. */
final class Numerics {
    private Numerics() {}
    static double mean(double[] a){return Arrays.stream(a).average().orElse(0);}
    static double quantile(double[] values,double q){if(values.length==0)return 0;double[] a=values.clone();Arrays.sort(a);double pos=(a.length-1)*q;int lo=(int)pos,hi=Math.min(a.length-1,lo+1);return a[lo]+(pos-lo)*(a[hi]-a[lo]);}
    static double dot(double[] a,double[] b){double x=0;for(int i=0;i<a.length;i++)x+=a[i]*b[i];return x;}
    /** 대칭 행렬의 Jacobi 고유분해로 작은 least-squares의 최소 norm 해를 계산한다. */
    static double[] leastSquares(double[][] x,double[] y){
        if(x.length==0||x.length!=y.length)throw new IllegalArgumentException("empty/ragged regression");int n=x[0].length;
        double[][] a=new double[n][n],v=new double[n][n];double[] b=new double[n];
        for(int i=0;i<n;i++)v[i][i]=1;
        for(int r=0;r<x.length;r++){if(x[r].length!=n)throw new IllegalArgumentException("ragged regression");for(int i=0;i<n;i++){b[i]+=x[r][i]*y[r];for(int j=0;j<n;j++)a[i][j]+=x[r][i]*x[r][j];}}
        for(int iteration=0;iteration<100*n*n;iteration++){
            int p=0,q=0;double largest=0,scale=0;for(int i=0;i<n;i++){scale=Math.max(scale,Math.abs(a[i][i]));for(int j=i+1;j<n;j++)if(Math.abs(a[i][j])>largest){largest=Math.abs(a[i][j]);p=i;q=j;}}
            if(largest<=Math.max(1e-14,scale*1e-14))break;
            double angle=.5*Math.atan2(2*a[p][q],a[q][q]-a[p][p]),s=Math.sin(angle),c=Math.cos(angle);
            double pp=a[p][p],qq=a[q][q],pq=a[p][q];
            for(int i=0;i<n;i++)if(i!=p&&i!=q){double ip=a[i][p],iq=a[i][q];a[i][p]=a[p][i]=c*ip-s*iq;a[i][q]=a[q][i]=s*ip+c*iq;}
            a[p][p]=c*c*pp-2*s*c*pq+s*s*qq;a[q][q]=s*s*pp+2*s*c*pq+c*c*qq;a[p][q]=a[q][p]=0;
            for(int i=0;i<n;i++){double ip=v[i][p],iq=v[i][q];v[i][p]=c*ip-s*iq;v[i][q]=s*ip+c*iq;}
        }
        double max=0;for(int i=0;i<n;i++)max=Math.max(max,Math.abs(a[i][i]));double[] answer=new double[n];
        for(int k=0;k<n;k++)if(a[k][k]>Math.max(1e-14,max*1e-12)){
            double projection=0;for(int i=0;i<n;i++)projection+=v[i][k]*b[i];projection/=a[k][k];for(int i=0;i<n;i++)answer[i]+=v[i][k]*projection;
        }
        return answer;
    }
    static double sigmoid(double x){return x>=0?1/(1+Math.exp(-x)):Math.exp(x)/(1+Math.exp(x));}
    /** sklearn C=1000의 이항 logistic 목적식: summed log loss + ||w||²/(2C), intercept 비정규화. */
    static double[] logistic(double[][] x,double[] y){
        double[] b=new double[3];
        for(int it=0;it<200;it++){
            double[] gradient={b[0]/1000,b[1]/1000,0};double[][] h={{.001,0,0},{0,.001,0},{0,0,1e-10}};
            for(int r=0;r<x.length;r++){double[] row={x[r][0]/100,x[r][1]/100,1};double p=sigmoid(dot(row,b)),w=Math.max(1e-12,p*(1-p));
                for(int i=0;i<3;i++){gradient[i]+=(p-y[r])*row[i];for(int j=0;j<3;j++)h[i][j]+=w*row[i]*row[j];}}
            double[] step=leastSquares(h,gradient);double before=logisticLoss(x,y,b),factor=1;double[] candidate=new double[3];
            while(factor>=1e-8){for(int i=0;i<3;i++)candidate[i]=b[i]-factor*step[i];if(logisticLoss(x,y,candidate)<=before)break;factor/=2;}
            if(factor<1e-8)break;double difference=0;for(int i=0;i<3;i++){difference=Math.max(difference,Math.abs(candidate[i]-b[i]));b[i]=candidate[i];}if(difference<1e-9)break;
        }
        return b;
    }
    private static double logisticLoss(double[][] x,double[] y,double[] b){double value=(b[0]*b[0]+b[1]*b[1])/2000;for(int i=0;i<x.length;i++){double z=b[0]*x[i][0]/100+b[1]*x[i][1]/100+b[2];value+=Math.max(z,0)-z*y[i]+Math.log1p(Math.exp(-Math.abs(z)));}return value;}
    /** conditional ARIMA(1,1,1), phi/theta ∈ [-.98,.98]. bounded LM, 과거 64개만 사용. */
    static double[] arima(double[] history,int steps){
        if(history.length<3)throw new IllegalArgumentException("ARIMA needs history");int first=Math.max(0,history.length-64);double[] d=new double[history.length-first-1];for(int i=0;i<d.length;i++)d[i]=history[first+i+1]-history[first+i];
        double phi=0,theta=0,lambda=1e-3;
        for(int it=0;it<60;it++){
            double[] e=residual(d,phi,theta),ep=residual(d,phi+1e-5,theta),et=residual(d,phi,theta+1e-5);
            double a=lambda,b=0,c=lambda,g=0,h=0;
            for(int i=1;i<e.length;i++){double jp=(ep[i]-e[i])/1e-5,jt=(et[i]-e[i])/1e-5;a+=jp*jp;b+=jp*jt;c+=jt*jt;g+=jp*e[i];h+=jt*e[i];}
            double determinant=a*c-b*b;if(determinant<=1e-20)break;double dp=(c*g-b*h)/determinant,dt=(a*h-b*g)/determinant;
            double np=ScalingSupport.clip(phi-dp,-.98,.98),nt=ScalingSupport.clip(theta-dt,-.98,.98);
            if(dot(residual(d,np,nt),residual(d,np,nt))<dot(e,e)){phi=np;theta=nt;lambda=Math.max(1e-9,lambda/3);if(Math.abs(dp)+Math.abs(dt)<1e-9)break;}else lambda*=10;
        }
        double[] e=residual(d,phi,theta);double change=phi*d[d.length-1]+theta*e[e.length-1],value=history[history.length-1];for(int i=0;i<steps;i++){value+=change;change*=phi;}return new double[]{Math.max(0,value),phi,theta};
    }
    private static double[] residual(double[] d,double phi,double theta){double[] e=new double[d.length];for(int i=1;i<e.length;i++)e[i]=d[i]-phi*d[i-1]-theta*e[i-1];return e;}
    static int[] twoMeans(double[][] points){
        int n=points.length,p=points[0].length;double[] scale=new double[p];double[][] x=new double[n][p];
        for(int j=0;j<p;j++){double mean=0;for(var r:points)mean+=r[j]/n;for(var r:points)scale[j]+=(r[j]-mean)*(r[j]-mean)/n;scale[j]=Math.max(1e-6,Math.sqrt(scale[j]));for(int i=0;i<n;i++)x[i][j]=points[i][j]/scale[j];}
        // 결정적인 두 seed. 초기화 차이를 Python KMeans와 동일하다고 표시하지 않는다.
        double[][] centers={x[0].clone(),x[0].clone()};double distance=-1;for(var r:x){double d=0;for(int j=0;j<p;j++)d+=(r[j]-centers[0][j])*(r[j]-centers[0][j]);if(d>distance){distance=d;centers[1]=r.clone();}}
        int[] labels=new int[n];Arrays.fill(labels,-1);
        for(int it=0;it<100;it++){boolean changed=false;double[][] sums=new double[2][p];int[] counts=new int[2];
            for(int i=0;i<n;i++){double da=0,db=0;for(int j=0;j<p;j++){da+=Math.pow(x[i][j]-centers[0][j],2);db+=Math.pow(x[i][j]-centers[1][j],2);}int label=db<da?1:0;changed|=labels[i]!=label;labels[i]=label;counts[label]++;for(int j=0;j<p;j++)sums[label][j]+=x[i][j];}
            for(int k=0;k<2;k++)if(counts[k]>0)for(int j=0;j<p;j++)centers[k][j]=sums[k][j]/counts[k];if(!changed)break;
        }return labels;
    }
}
