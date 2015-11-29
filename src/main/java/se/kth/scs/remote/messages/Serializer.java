package se.kth.scs.remote.messages;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Collection;
import java.util.LinkedList;
import se.kth.scs.partitioning.ConcurrentVertex;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class Serializer {

  public static int[] deserializeRequest(DataInputStream input) throws IOException {
    int n = input.readInt();
    byte[] buffer = readNexBytes(input, n);
    IntBuffer intBuffer = ByteBuffer.wrap(buffer).asIntBuffer();
    int[] array = new int[intBuffer.remaining()];
    intBuffer.get(array);
    return array;
  }

  public static void serializeRequest(DataOutputStream output, byte type, int[] request) throws IOException {
    int size = request.length * 4;
    byte[] bytes = new byte[size];
    IntBuffer buffer = ByteBuffer.wrap(bytes).asIntBuffer();
    for (int r : request) {
      buffer.put(r);
    }

    write(output, type, size, bytes);
  }

  private static byte[] readNexBytes(DataInputStream input, int n) throws IOException {
    byte[] buffer = new byte[n];
    while (n > 0) {
      n -= input.read(buffer, buffer.length - n, n);
    }

    return buffer;
  }

  public static void serializeVerticesReadResponse(DataOutputStream output, LinkedList<Vertex> vertices) throws IOException {
//    byte type = Protocol.VERTICES_READ_RESPONSE;
    int size = vertices.size() * 3 * 4;
    byte[] response = new byte[size];
    IntBuffer buffer = ByteBuffer.wrap(response).asIntBuffer();
    for (Vertex v : vertices) {
      buffer.put(v.getId());
      buffer.put(v.getpDegree());
      buffer.put(v.getPartitions());
    }
    write(output, size, response);
  }

  public static void serializePartitionsReadResponse(DataOutputStream output, int[] edgeSizes) throws IOException {
//    byte type = Protocol.PARTITIONS_RESPONSE;
    int size = edgeSizes.length * 4;
    byte[] response = new byte[size];
    IntBuffer buffer = ByteBuffer.wrap(response).asIntBuffer();
    for (int e : edgeSizes) {
      buffer.put(e);
    }
    write(output, size, response);
  }

  private static void write(DataOutputStream output, byte type, int size, byte[] response) throws IOException {
    output.writeByte(type);
    output.writeInt(size);
    output.write(response);
    output.flush();
  }

  private static void write(DataOutputStream output, int size, byte[] response) throws IOException {
    output.writeInt(size);
    output.write(response);
    output.flush();
  }

  public static void serializeAllVerticesReadResponse(DataOutputStream output, int[] array) throws IOException {
    // this is a reduntant version of serializeVerticesReadResponse but only not to duplicate the memory usage.
    int size = array.length * 4;
    byte[] response = new byte[size];
    IntBuffer buffer = ByteBuffer.wrap(response).asIntBuffer();
    for (int i = 0; i < array.length; i = i + 3) {
      buffer.put(array[i]);
      buffer.put(array[i + 1]);
      buffer.put(array[i + 2]);
    }
    write(output, size, response);
  }

  public static int deserializeAllVerticesRequest(DataInputStream input) throws IOException {
    return input.readInt();
  }
}
