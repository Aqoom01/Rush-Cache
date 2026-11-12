package io.openkache.server.ionia_eighth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.server.ionia.*;
import java.io.*;
import java.util.*;
import org.junit.jupiter.api.*;

class IoniaMetaBatchCodecTest {
    static final NodeId A=new NodeId(1);static final ClientId C=new ClientId(9);
    static IoniaClientTransport.Request request(Command c){return new IoniaClientTransport.Request(0,c,C,new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,1234);}
    static byte[] encode(IoniaWire.Writer writer)throws IOException{var bytes=new ByteArrayOutputStream();try(var out=new DataOutputStream(bytes)){writer.write(out);}return bytes.toByteArray();}
    static DataInputStream input(byte[] bytes){return new DataInputStream(new ByteArrayInputStream(bytes));}
    @Test void requestsAndMixedIndexDataMissAndErrorRoundTrip() throws Exception{
        var items=List.of(new IoniaMetaBatchTransport.Item(1,request(new Command.Get(Bytes.utf8("k")))),new IoniaMetaBatchTransport.Item(2,request(new Command.Get(Bytes.utf8("k2")))));
        assertEquals(items,IoniaMetaBatchCodec.requests(input(encode(out->IoniaMetaBatchCodec.requests(out,items)))));
        var values=new ArrayList<IoniaMetaBatchTransport.Answer>();int i=0;
        for(Result data:Arrays.asList(null,Result.ok(Bytes.utf8("v"),3,RoutingVersion.ZERO,OwnershipEpoch.ZERO),Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO),Result.of(ResultStatus.TIMEOUT,RoutingVersion.ZERO,OwnershipEpoch.ZERO)))
            values.add(new IoniaMetaBatchTransport.Answer(++i,new IoniaClientTransport.MetaReply(A,0,C,new OperationId(i),4,data)));
        assertEquals(values,IoniaMetaBatchCodec.answers(A,input(encode(out->IoniaMetaBatchCodec.answers(out,values)))));
    }
    @Test void malformedRequestsFailBeforeEngineDispatch() throws Exception{
        for(int count:new int[]{-1,0,65,Integer.MAX_VALUE})assertThrows(IOException.class,()->IoniaMetaBatchCodec.requests(input(encode(out->out.writeInt(count)))));
        var item=new IoniaMetaBatchTransport.Item(1,request(new Command.Get(Bytes.utf8("k"))));
        assertThrows(IllegalArgumentException.class,()->IoniaMetaBatchCodec.validate(List.of(item,item)));
        assertThrows(IllegalArgumentException.class,()->new IoniaMetaBatchTransport.Item(1,request(new Command.Set(Bytes.utf8("k"),Bytes.utf8("v")))));
        var body=encode(out->{IoniaMetaBatchCodec.requests(out,List.of(item));out.writeByte(0);});
        assertThrows(IOException.class,()->IoniaMetaBatchCodec.requests(input(body)));
        byte[] encoded=encode(out->IoniaMetaBatchCodec.requests(out,List.of(item)));assertThrows(IOException.class,()->IoniaMetaBatchCodec.requests(input(Arrays.copyOf(encoded,encoded.length-1))));
        var huge=new IoniaMetaBatchTransport.Item(2,request(new Command.Get(Bytes.copyOf(new byte[1024*1024]))));
        assertThrows(IllegalArgumentException.class,()->IoniaMetaBatchCodec.validate(List.of(huge)));
    }
    @Test void malformedAnswerCountsDuplicateTokensAndTrailingDataAreRejected() throws Exception{
        for(int count:new int[]{-1,0,65,Integer.MAX_VALUE})assertThrows(IOException.class,()->IoniaMetaBatchCodec.answers(A,input(encode(out->out.writeInt(count)))));
        var answer=new IoniaMetaBatchTransport.Answer(1,new IoniaClientTransport.MetaReply(A,0,C,new OperationId(1),0,null));
        assertThrows(IOException.class,()->IoniaMetaBatchCodec.answers(A,input(encode(out->IoniaMetaBatchCodec.answers(out,List.of(answer,answer))))));
        assertThrows(IOException.class,()->IoniaMetaBatchCodec.answers(A,input(encode(out->{IoniaMetaBatchCodec.answers(out,List.of(answer));out.writeByte(0);}))));
    }
    @Test void responseBudgetPreservesIdentityAndReportsOverflowAsFailure() throws Exception{
        Result big=Result.ok(Bytes.copyOf(new byte[31*1024*1024]),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO);
        var first=new IoniaMetaBatchTransport.Answer(1,new IoniaClientTransport.MetaReply(A,0,C,new OperationId(1),0,big));
        var second=new IoniaMetaBatchTransport.Answer(2,new IoniaClientTransport.MetaReply(A,0,C,new OperationId(2),0,big));
        byte[] body=encode(out->IoniaMetaBatchCodec.answers(out,List.of(first,second)));assertTrue(body.length<=IoniaMetaBatchCodec.MAX_REPLY_BYTES);
        var decoded=IoniaMetaBatchCodec.answers(A,input(body));assertEquals(ResultStatus.OK,decoded.getFirst().reply().data().status());
        assertEquals(ResultStatus.UNAVAILABLE,decoded.getLast().reply().data().status());assertEquals(2,decoded.getLast().token());assertEquals(new OperationId(2),decoded.getLast().reply().operationId());
    }
}
