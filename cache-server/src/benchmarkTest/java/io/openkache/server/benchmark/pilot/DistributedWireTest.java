package io.openkache.server.benchmark.pilot;

import java.io.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedWireTest {
    @Test void boundedFrameRoundTripPreservesUnicodeAndEscapedProperties()throws Exception{
        var p=DistributedWire.packet("HELLO");p.setProperty("value","a=b\n한글");var out=new ByteArrayOutputStream();DistributedWire.write(out,p);assertEquals(p,DistributedWire.read(new ByteArrayInputStream(out.toByteArray())));
    }
    @Test void oversizedNegativeAndTruncatedFramesAreRejected()throws Exception{
        for(int n:new int[]{-1,0,DistributedWire.MAX_FRAME+1,10}){var bytes=new ByteArrayOutputStream();new DataOutputStream(bytes).writeInt(n);assertThrows(IOException.class,()->DistributedWire.read(new ByteArrayInputStream(bytes.toByteArray())));}
    }
    static Properties hello(){var p=DistributedWire.packet("HELLO");for(String k:List.of("protocol","run.id","contract.sha256","input.sha256","bundle.sha256","control.token"))p.setProperty(k,"expected");p.setProperty("slot","0");p.setProperty("stream","0");p.setProperty("wire.base","10000000");p.setProperty("pid","1");p.setProperty("java.feature","21");p.setProperty("instance.id",UUID.randomUUID().toString());return p;}
    @Test void wrongRunCodeTokenAndDuplicateSlotCannotRegister()throws Exception{
        var expected=hello();for(String key:List.of("run.id","contract.sha256","bundle.sha256","control.token","wire.base","java.feature")){var h=hello();h.setProperty(key,"0");assertThrows(IOException.class,()->DistributedPilotMain.validateHello(h,expected,new HashSet<>()));}
        Set<Integer> seen=new HashSet<>();assertEquals(0,DistributedPilotMain.validateHello(hello(),expected,seen));assertThrows(IOException.class,()->DistributedPilotMain.validateHello(hello(),expected,seen));
    }
    @Test void absentPacketKindAndMissingFieldsAreRejected(){
        assertThrows(IOException.class,()->DistributedWire.required(new Properties(),"kind"));assertThrows(IOException.class,()->DistributedWire.number(new Properties(),"slot"));
    }
}
