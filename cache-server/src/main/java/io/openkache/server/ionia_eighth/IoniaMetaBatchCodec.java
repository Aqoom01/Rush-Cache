package io.openkache.server.ionia_eighth;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.server.ionia.IoniaWire;
import java.io.*;
import java.util.*;

/** New opcode inside the bounded O00 envelope. Peer/storage formats are reused unchanged.
 * The whole batch is decoded/validated before engine dispatch or client publication.
 */
public final class IoniaMetaBatchCodec {
    public static final int META_BATCH=7, MAX_REPLY_BYTES=60*1024*1024;
    private IoniaMetaBatchCodec() { }
    public static void validate(List<IoniaMetaBatchTransport.Item> items){
        if(items.isEmpty()||items.size()>IoniaMetaBatchSettings.WIRE_ENTRIES)throw new IllegalArgumentException("invalid Meta batch count");
        long bytes=4,view=items.getFirst().request().view();Set<Long> tokens=new HashSet<>();
        for(var item:items){if(!tokens.add(item.token())||item.request().view()!=view)throw new IllegalArgumentException("duplicate token/mixed View");bytes+=item.encodedBytes();}
        if(bytes>IoniaMetaBatchSettings.WIRE_REQUEST_BYTES)throw new IllegalArgumentException("Meta batch request too large");
    }
    public static void requests(DataOutputStream out,List<IoniaMetaBatchTransport.Item> items)throws IOException{
        validate(items);out.writeInt(items.size());for(var item:items){out.writeLong(item.token());IoniaWire.request(out,item.request());}
    }
    public static List<IoniaMetaBatchTransport.Item> requests(DataInputStream in)throws IOException{
        if(in.available()>IoniaMetaBatchSettings.WIRE_REQUEST_BYTES)throw new IOException("Meta batch request too large");
        int count=in.readInt();if(count<1||count>IoniaMetaBatchSettings.WIRE_ENTRIES||count>in.available()/61)throw new IOException("invalid Meta batch count");
        List<IoniaMetaBatchTransport.Item> items=new ArrayList<>(count);
        try{for(int i=0;i<count;i++)items.add(new IoniaMetaBatchTransport.Item(in.readLong(),IoniaWire.request(in)));validate(items);}
        catch(IllegalArgumentException error){throw new IOException("invalid Meta batch",error);}
        IoniaWire.exhausted(in);return List.copyOf(items);
    }
    public static void answers(DataOutputStream out,List<IoniaMetaBatchTransport.Answer> answers)throws IOException{
        if(answers.isEmpty()||answers.size()>IoniaMetaBatchSettings.WIRE_ENTRIES)throw new IOException("invalid answer count");
        // Reserve one small error per remaining item: oversized DATA becomes explicit UNAVAILABLE, never an INDEX or successful MISS.
        int used=4;out.writeInt(answers.size());
        for(int i=0;i<answers.size();i++){
            var a=answers.get(i);byte[] encoded=answerBytes(a);
            if(used+encoded.length+128*(answers.size()-i-1)>MAX_REPLY_BYTES){
                var r=a.reply();a=new IoniaMetaBatchTransport.Answer(a.token(),new IoniaClientTransport.MetaReply(r.nodeId(),r.view(),r.clientId(),r.operationId(),0,
                        Result.of(ResultStatus.UNAVAILABLE,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));encoded=answerBytes(a);
            }
            out.write(encoded);used+=encoded.length;
        }
    }
    private static byte[] answerBytes(IoniaMetaBatchTransport.Answer a)throws IOException{
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();try(DataOutputStream out=new DataOutputStream(bytes)){
            var r=a.reply();out.writeLong(a.token());IoniaWire.identity(out,r.view(),r.clientId(),r.operationId());out.writeLong(r.index());out.writeBoolean(r.data()!=null);if(r.data()!=null)IoniaWire.result(out,r.data());
        }return bytes.toByteArray();
    }
    public static List<IoniaMetaBatchTransport.Answer> answers(NodeId node,DataInputStream in)throws IOException{
        if(in.available()>MAX_REPLY_BYTES)throw new IOException("Meta batch reply too large");
        int count=in.readInt();if(count<1||count>IoniaMetaBatchSettings.WIRE_ENTRIES||count>in.available()/41)throw new IOException("invalid answer count");
        List<IoniaMetaBatchTransport.Answer> result=new ArrayList<>(count);Set<Long> tokens=new HashSet<>();
        try{for(int i=0;i<count;i++){
            long token=in.readLong();if(!tokens.add(token))throw new IOException("duplicate answer token");
            long view=in.readLong();ClientId client=new ClientId(in.readLong());OperationId op=new OperationId(in.readLong());long index=in.readLong();
            result.add(new IoniaMetaBatchTransport.Answer(token,new IoniaClientTransport.MetaReply(node,view,client,op,index,in.readBoolean()?IoniaWire.result(in):null)));
        }}catch(IllegalArgumentException error){throw new IOException("invalid Meta answer",error);}
        IoniaWire.exhausted(in);return List.copyOf(result);
    }
}
