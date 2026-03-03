package com.distributed.network;

import java.io.*;
import java.net.Socket;

/**
 * Utility class for sending/receiving serialized Java objects over TCP sockets.
 * Uses length-prefixed framing to delimit messages.
 */
public class NetworkProtocol {

    /**
     * Send a serializable object over a socket.
     */
    public static void sendObject(Socket socket, Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        byte[] payload = baos.toByteArray();

        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(payload.length);
        dos.write(payload);
        dos.flush();
    }

    /**
     * Receive a serializable object from a socket.
     */
    public static Object receiveObject(Socket socket) throws IOException, ClassNotFoundException {
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        int length = dis.readInt();

        byte[] payload = new byte[length];
        dis.readFully(payload);

        ByteArrayInputStream bais = new ByteArrayInputStream(payload);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return ois.readObject();
    }

    /**
     * Send a heartbeat ping and receive stats.
     */
    public static final String HEARTBEAT_REQUEST = "HEARTBEAT_PING";
    public static final String TASK_REQUEST = "TASK_REQUEST";
}
