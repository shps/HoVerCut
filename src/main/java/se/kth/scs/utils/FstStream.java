package se.kth.scs.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.nustaq.serialization.FSTConfiguration;

/**
 * To use Fast-Serialization framework for serialization of messages.
 *
 * @author Hooman
 */
public class FstStream {

  static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

  public static Object readObject(Socket socket) throws IOException, ClassNotFoundException {
    DataInputStream stream = new DataInputStream(socket.getInputStream());
    int len = stream.readInt();
    byte buffer[] = new byte[len];
    while (len > 0) {
      len -= stream.read(buffer, buffer.length - len, len);
    }
    return conf.getObjectInput(buffer).readObject();
  }

  public static void writeObject(Object object, Socket socket) throws IOException {
    DataOutputStream output = new DataOutputStream(socket.getOutputStream());
    byte[] barray = conf.asByteArray(object);
    output.writeInt(barray.length);
    output.write(barray);
    output.flush();
  }
}
