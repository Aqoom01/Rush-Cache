package io.openkache.core.migration.experiment;

import io.openkache.core.migration.strategy.B3Csv;
import io.openkache.core.migration.strategy.B3Json;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.imageio.ImageIO;

/** 기존 S1~S5 요약의 JDK Java2D PNG 재생성. 원본 수치·min/max 의미를 유지한다. */
public final class ComparisonPlotMain {
    private static final List<String> COPIES=List.of("stop-and-copy","pre-copy","post-copy");
    private static final Color[] COLORS={new Color(0x4477AA),new Color(0xEE7733),new Color(0x228833)};
    private static final String FONT_FAMILY=fontFamily();
    private static final String[][] FIELDS={{"쓰기 차단 최소 tick","쓰기 차단 최대 tick","쓰기 차단 (tick)"},{"총 시간 최소 tick","총 시간 최대 tick","전체 이동 시간 (tick)"},{"전송량 비율 최소","전송량 비율 최대","전송량 / 초기 데이터"},{"조회 BUSY 비율 최소","조회 BUSY 비율 최대","조회 BUSY (%)"}};
    private record Panel(String title,List<String> cases,List<String> labels,int metric,boolean log,boolean bars){}
    private ComparisonPlotMain(){}
    private static String fontFamily(){for(String name:List.of("Malgun Gothic","Noto Sans CJK KR","NanumGothic","Dialog"))if(new Font(name,Font.PLAIN,14).canDisplayUpTo("쓰기 차단 삭제 조회")<0)return name;throw new IllegalStateException("한글을 표시할 수 있는 JDK 폰트가 필요합니다");}
    public static void main(String[] args) throws IOException {
        if(args.length!=2)throw new IllegalArgumentException("ComparisonPlotMain <summary.csv> <output-folder>; 원본 수치의 그림만 생성");Path summary=Path.of(args[0]),output=Path.of(args[1]);var index=load(summary);Files.createDirectories(output);var charts=new LinkedHashMap<String,List<Panel>>();
        var size=List.of("10000","100000","1000000");var sizePanels=new ArrayList<Panel>();for(String scenario:List.of("S1","S2"))for(int metric:new int[]{0,1})sizePanels.add(new Panel(scenario+" · "+FIELDS[metric][2],size.stream().map(n->scenario+"-n"+n).toList(),List.of("1만","10만","100만"),metric,true,false));charts.put("s1-s2-size.png",sizePanels);
        var rho=List.of("0.01","0.1","0.5","0.9","1.2");var load=new ArrayList<Panel>();for(int metric=0;metric<4;metric++)load.add(new Panel(FIELDS[metric][2],rho.stream().map(r->"S3-rho"+r).toList(),rho,metric,false,false));charts.put("s3-write-load.png",load);
        var post=new ArrayList<Panel>();for(int metric:new int[]{0,1,3})post.add(new Panel(FIELDS[metric][2],List.of("S1-n100000","S3-rho0.5"),List.of("쓰기 없음","rho=0.5"),metric,metric==0,true));charts.put("post-copy-tradeoff.png",post);
        var skew=new ArrayList<Panel>();for(int metric:new int[]{2,1,3})skew.add(new Panel(FIELDS[metric][2],List.of("S4-theta0.0","S4-theta0.99","S4-theta1.2"),List.of("Uniform","Zipf .99","Zipf 1.2"),metric,false,false));charts.put("s4-request-skew.png",skew);
        var deletes=new ArrayList<Panel>();for(int metric:new int[]{2,1,0})deletes.add(new Panel(FIELDS[metric][2],List.of("S5-delete0","S5-delete30"),List.of("DELETE 0%","DELETE 30%"),metric,metric==0,false));charts.put("s5-delete-ratio.png",deletes);
        var images=new ArrayList<Map<String,Object>>();for(var chart:charts.entrySet()){var image=render(index,chart.getValue());Path path=output.resolve(chart.getKey());if(!ImageIO.write(image,"png",path.toFile()))throw new IOException("PNG writer unavailable");images.add(Map.of("file",chart.getKey(),"sha256",B3ResearchLauncher.sha(Files.readAllBytes(path))));}
        Files.writeString(output.resolve("sources.json"),B3Json.encode(Map.of("generator","b3-java2d-plot-1","sourceSummarySha256",B3ResearchLauncher.sha(Files.readAllBytes(summary)),"range","seed min/max; center=(min+max)/2, not seed mean or median","figures",images))+"\n",StandardCharsets.UTF_8);
    }
    static Map<String,Map<String,String>> load(Path summary) throws IOException {var rows=B3Csv.read(Files.readString(summary,StandardCharsets.UTF_8));if(rows.size()!=48)throw new IllegalArgumentException("reference summary needs 48 rows");var index=new HashMap<String,Map<String,String>>();for(var row:rows){String id=row.get("조건 ID")+":"+row.get("방식");if(!COPIES.contains(row.get("방식"))||index.put(id,row)!=null||!row.get("seed 수").equals("5")||Double.parseDouble(row.get("정합성 위반"))!=0)throw new IllegalArgumentException("unverified/duplicate plot row");for(int metric=0;metric<4;metric++){double low=Double.parseDouble(row.get(FIELDS[metric][0])),high=Double.parseDouble(row.get(FIELDS[metric][1]));if(!Double.isFinite(low)||!Double.isFinite(high)||low<0||low>high)throw new IllegalArgumentException("invalid plot range");}}return index;}
    private static BufferedImage render(Map<String,Map<String,String>> index,List<Panel> panels){int columns=panels.size()==4?2:3,rows=panels.size()==4?2:1,width=columns*540,height=rows*370+135;var image=new BufferedImage(width,height,BufferedImage.TYPE_INT_RGB);var g=image.createGraphics();try{g.setRenderingHint(RenderingHints.KEY_ANTIALIASING,RenderingHints.VALUE_ANTIALIAS_ON);g.setColor(Color.WHITE);g.fillRect(0,0,width,height);g.setFont(new Font(FONT_FAMILY,Font.BOLD,22));g.setColor(Color.DARK_GRAY);g.drawString("B3 · 복사 방식 비교",32,34);for(int i=0;i<panels.size();i++)panel(g,index,panels.get(i),(i%columns)*540,(i/columns)*370+45);g.setFont(new Font(FONT_FAMILY,Font.PLAIN,15));for(int i=0;i<3;i++){g.setColor(COLORS[i]);g.fillRect(40+i*350,height-57,20,10);g.drawString(COPIES.get(i),70+i*350,height-47);}g.setColor(Color.DARK_GRAY);g.drawString("가상 시간 · 선/막대=(min+max)/2 · 오차막대=seed 5개 min~max · log 표기는 y축에 적용",32,height-17);}finally{g.dispose();}return image;}
    private static void panel(Graphics2D g,Map<String,Map<String,String>> index,Panel p,int x,int y){int left=x+86,top=y+45,w=420,h=245;double min=p.log()?Double.POSITIVE_INFINITY:0,max=0;double[][] low=new double[3][p.cases().size()],high=new double[3][p.cases().size()];
        for(int s=0;s<3;s++)for(int i=0;i<p.cases().size();i++){var row=index.get(p.cases().get(i)+":"+COPIES.get(s));if(row==null)throw new IllegalArgumentException("missing plot condition "+p.cases().get(i));double multiplier=p.metric()==3?100:1;low[s][i]=Double.parseDouble(row.get(FIELDS[p.metric()][0]))*multiplier;high[s][i]=Double.parseDouble(row.get(FIELDS[p.metric()][1]))*multiplier;if(p.log()&&low[s][i]<=0)throw new IllegalArgumentException("log plot needs positive values");if(p.log())min=Math.min(min,low[s][i]*.7);max=Math.max(max,high[s][i]);}
        max=Math.max(max*(p.log()?1.5:1.15),p.log()?min*2:1);double lo=p.log()?Math.log10(min):min,hi=p.log()?Math.log10(max):max;g.setColor(Color.DARK_GRAY);g.setFont(new Font(FONT_FAMILY,Font.BOLD,16));g.drawString(p.title()+(p.log()?" · log y":""),x+30,y+20);g.setFont(new Font(FONT_FAMILY,Font.PLAIN,13));
        for(int k=0;k<=4;k++){int py=top+h-k*h/4;g.setColor(new Color(0xDDDDDD));g.drawLine(left,py,left+w,py);g.setColor(Color.DARK_GRAY);double value=lo+(hi-lo)*k/4;g.drawString(String.format(java.util.Locale.ROOT,"%.3g",p.log()?Math.pow(10,value):value),x+8,py+4);}
        g.drawLine(left,top,left,top+h);g.drawLine(left,top+h,left+w,top+h);int count=p.cases().size();
        for(int i=0;i<count;i++){int px=left+(i+1)*w/(count+1);g.drawString(p.labels().get(i),px-30,top+h+25);}
        for(int s=0;s<3;s++){g.setColor(COLORS[s]);g.setStroke(new BasicStroke(2));var path=new Path2D.Double();for(int i=0;i<count;i++){double center=(low[s][i]+high[s][i])/2;int px=left+(i+1)*w/(count+1)+(p.bars()?(s-1)*28:0),a=coord(high[s][i],lo,hi,top,h,p.log()),b=coord(low[s][i],lo,hi,top,h,p.log()),mid=coord(center,lo,hi,top,h,p.log());g.drawLine(px,a,px,b);g.drawLine(px-5,a,px+5,a);g.drawLine(px-5,b,px+5,b);if(p.bars()){g.fillRect(px-10,mid,20,top+h-mid);}else{if(i==0)path.moveTo(px,mid);else path.lineTo(px,mid);g.fillOval(px-4,mid-4,8,8);}}if(!p.bars())g.draw(path);}
    }
    private static int coord(double value,double lo,double hi,int top,int h,boolean log){double v=log?Math.log10(value):value;return top+h-(int)Math.round((v-lo)/(hi-lo)*h);}
}
