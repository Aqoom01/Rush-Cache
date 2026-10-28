package com.openkache.failuredetector.transport;

import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UdpWireMessageTest {
    @Test
    void versionedFrameRoundTripsWithDeclaredWireLength() {
        UdpWireMessage source = new UdpWireMessage(
                UdpWireMessage.Type.PING_REQUEST, 42, 0, 0, 1);

        byte[] encoded = source.encode();

        assertEquals(UdpWireMessage.WIRE_BYTES, encoded.length);
        assertEquals(source, UdpWireMessage.decode(encoded, encoded.length));
        assertThrows(IllegalArgumentException.class, () -> UdpWireMessage.decode(encoded, 3));
    }

    @Test
    void frameTraversesARealLoopbackUdpSocketWithoutChangingByteCount() throws Exception {
        UdpWireMessage source = new UdpWireMessage(
                UdpWireMessage.Type.HEARTBEAT, 7, 1, 1, 0);
        byte[] encoded = source.encode();
        try (DatagramSocket receiver = new DatagramSocket(0, InetAddress.getLoopbackAddress());
                DatagramSocket sender = new DatagramSocket()) {
            sender.send(new DatagramPacket(
                    encoded,
                    encoded.length,
                    InetAddress.getLoopbackAddress(),
                    receiver.getLocalPort()
            ));
            byte[] buffer = new byte[UdpWireMessage.WIRE_BYTES];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            receiver.receive(packet);

            assertEquals(UdpWireMessage.WIRE_BYTES, packet.getLength());
            assertEquals(source, UdpWireMessage.decode(packet.getData(), packet.getLength()));
        }
    }
}
