package io.openkache.server.ionia_fourth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.server.ionia.IoniaWire;
import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_fourth.*;
import java.io.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaPrepareSizerWireTest {
    @Test void V03_02_actualEncoderAgreesWithSizerIncludingOuterLengthAndModifiedUtf() throws Exception {
        int cases=0;
        for(String configuration:List.of("cfg","한글","a\u0000b","😀","a".repeat(64)))
            for(int count:new int[]{1,2,3,7}) for(int keyLength:new int[]{0,1,31,255}) for(int valueLength:new int[]{0,1,127,8192}) {
                List<IoniaMessage.LogEntry> entries=new ArrayList<>();
                for(int i=1;i<=count;i++) {
                    Bytes key=Bytes.copyOf(new byte[keyLength]);
                    Command command=i%2==0 ? new Command.Delete(key):new Command.Set(key,Bytes.copyOf(new byte[valueLength]));
                    entries.add(new IoniaMessage.LogEntry(i,new ClientId(1),new OperationId(i),command));
                }
                var message=new IoniaMessage.Prepare(0,entries); ByteArrayOutputStream bytes=new ByteArrayOutputStream();
                IoniaWire.write(bytes,1,new NodeId(1),new NodeId(2),configuration,IoniaWire.PEER,out->IoniaWire.peer(out,message));
                assertEquals(new IoniaPrepareSizer(configuration).packetBytes(entries),bytes.size());
                var packet=IoniaWire.read(new ByteArrayInputStream(bytes.toByteArray()));
                assertEquals(message,IoniaWire.peer(packet.body())); IoniaWire.exhausted(packet.body()); cases++;
            }
        assertEquals(320,cases);
    }
}
