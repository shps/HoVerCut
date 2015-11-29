package se.kth.scs.remote;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import se.kth.scs.partitioning.Vertex;
import se.kth.scs.remote.messages.Protocol;
import se.kth.scs.remote.messages.Serializer;

/**
 *
 * @author Hooman
 */
public class QueryHandler implements Runnable {

  private final ServerStorage state;
  private final Socket socket;

  public QueryHandler(ServerStorage state, Socket socket) {
    this.state = state;
    this.socket = socket;
  }

  @Override
  public void run() {
    try {
      DataInputStream input = new DataInputStream(socket.getInputStream());
      DataOutputStream output = new DataOutputStream(socket.getOutputStream());
      while (true) {
        byte request = input.readByte();
        if (request == Protocol.VERTICES_READ_REQUEST) {
          int[] vids = Serializer.deserializeRequest(input);
          LinkedList<Vertex> response = state.getVertices(vids);
          Serializer.serializeVerticesReadResponse(output, response);
        } else if (request == Protocol.VERTICES_WRITE_REQUEST) {
          int[] vertices = Serializer.deserializeRequest(input);
          state.putVertices(vertices);
        } else if (request == Protocol.PARTITIONS_REQUEST) {
          int[] response = state.getPartitions();
          Serializer.serializePartitionsReadResponse(output, response);
        } else if (request == Protocol.PARTITIONS_WRITE_REQUEST) {
          int[] partitions = Serializer.deserializeRequest(input);
          state.putPartitions(partitions);
        } else if (request == Protocol.ALL_VERTICES_REQUEST) {
          int expectedSize = Serializer.deserializeAllVerticesRequest(input);
          int[] response = state.getAllVertices(expectedSize);
          Serializer.serializeAllVerticesReadResponse(output, response);
        } else if (request == Protocol.CLOSE_SESSION_REQUEST) {
          System.out.println("A close-session request is received.");
          break;
        } else if (request == Protocol.CLEAR_ALL_REQUEST) {
          state.releaseResources();
        } else {
          throw new Exception(String.format("Request type %d is not found.", request));
        }
      }
    } catch (Exception ex) {
      if (!(ex instanceof EOFException)) {
        ex.printStackTrace();
      }
    }

    try {
      System.out.println("Socket is closed.");
      if (!socket.isClosed()) {
        socket.close();
      }
    } catch (IOException ex) {
      if (!(ex instanceof EOFException)) {
        ex.printStackTrace();
      }
    }
  }
}
