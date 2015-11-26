package se.kth.scs.remote;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import se.kth.scs.remote.messages.AllVerticesRequest;
import se.kth.scs.remote.messages.ClearAllRequest;
import se.kth.scs.remote.messages.CloseSessionRequest;
import se.kth.scs.remote.messages.PartitionsRequest;
import se.kth.scs.remote.messages.PartitionsResponse;
import se.kth.scs.remote.messages.PartitionsWriteRequest;
import se.kth.scs.remote.messages.VerticesReadRequest;
import se.kth.scs.remote.messages.VerticesReadResponse;
import se.kth.scs.remote.messages.VerticesWriteRequest;
import utils.FstStream;

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
      while (true) {
        Object request = FstStream.readObject(socket);
        if (request instanceof VerticesReadRequest) {
          VerticesReadResponse response = state.getVertices((VerticesReadRequest) request);
          FstStream.writeObject(response, socket);
        } else if (request instanceof VerticesWriteRequest) {
          state.putVertices((VerticesWriteRequest) request);
        } else if (request instanceof PartitionsRequest) {
          PartitionsResponse response = state.getPartitions((PartitionsRequest) request);
          FstStream.writeObject(response, socket);
        } else if (request instanceof PartitionsWriteRequest) {
          state.putPartitions((PartitionsWriteRequest) request);
        } else if (request instanceof AllVerticesRequest) {
          VerticesReadResponse response = state.getAllVertices();
          FstStream.writeObject(response, socket);
        } else if (request instanceof CloseSessionRequest) {
          System.out.println("A close-session request is received.");
          break;
        } else if (request instanceof ClearAllRequest) {
          state.releaseResources();
        } else {
          throw new ClassNotFoundException(request.getClass().toString());
        }
      }
    } catch (IOException | ClassNotFoundException ex) {
      if (!(ex instanceof EOFException)) {
        ex.printStackTrace();
      }
    }

    try {
      System.out.println("Socket is closed.");
      socket.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
