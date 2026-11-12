package io.openkache.server.benchmark.pilot;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/** Bounded private controller protocol, separate from all replica wire/API formats. */
public final class DistributedWire {
    public static final int MAX_FRAME=4*1024*1024;
    private DistributedWire(){}
    public static Properties packet(String kind){var p=new Properties();p.setProperty("kind",kind);return p;}
    public static void put(Properties p,String key,Object value){p.setProperty(key,String.valueOf(value));}
    public static String required(Properties p,String key)throws IOException{String s=p.getProperty(key);if(s==null||s.isBlank())throw new IOException("missing control field: "+key);return s;}
    public static long number(Properties p,String key)throws IOException{try{return Long.parseLong(required(p,key));}catch(NumberFormatException e){throw new IOException("invalid control number: "+key,e);}}
    public static void kind(Properties p,String expected)throws IOException{if(!expected.equals(required(p,"kind")))throw new IOException("expected "+expected+", received "+p.getProperty("kind"));}
    public static void write(OutputStream stream,Properties p)throws IOException{
        StringWriter writer=new StringWriter();p.store(writer,"distributed pilot control v1");byte[] bytes=writer.toString().getBytes(StandardCharsets.UTF_8);if(bytes.length>MAX_FRAME)throw new IOException("control frame too large");
        var out=new DataOutputStream(stream);out.writeInt(bytes.length);out.write(bytes);out.flush();
    }
    public static Properties read(InputStream stream)throws IOException{
        var in=new DataInputStream(stream);int n=in.readInt();if(n<1||n>MAX_FRAME)throw new IOException("control frame size");byte[] bytes=in.readNBytes(n);if(bytes.length!=n)throw new EOFException("partial control frame");
        var p=new Properties();p.load(new StringReader(new String(bytes,StandardCharsets.UTF_8)));required(p,"kind");return p;
    }
}
