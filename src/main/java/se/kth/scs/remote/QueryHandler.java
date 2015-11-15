package se.kth.scs.remote;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import se.kth.scs.remote.requests.AllVerticesRequest;
import se.kth.scs.remote.requests.ClearAllRequest;
import se.kth.scs.remote.requests.CloseSessionRequest;
import se.kth.scs.remote.requests.PartitionsRequest;
import se.kth.scs.remote.requests.PartitionsResponse;
import se.kth.scs.remote.requests.PartitionsWriteRequest;
import se.kth.scs.remote.requests.VerticesReadRequest;
import se.kth.scs.remote.requests.VerticesReadResponse;
import se.kth.scs.remote.requests.VerticesWriteRequest;

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
    try (ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
      ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream())) {

      while (true) {
        Object request = input.readObject();
        if (request instanceof VerticesReadRequest) {
          VerticesReadResponse response = state.getVertices((VerticesReadRequest) request);
          output.writeObject(response);
          output.flush();
        } else if (request instanceof VerticesWriteRequest) {
          state.putVertices((VerticesWriteRequest) request);
        } else if (request instanceof PartitionsRequest) {
          PartitionsResponse response = state.getPartitions((PartitionsRequest) request);
          output.writeObject(response);
          output.flush();
        } else if (request instanceof PartitionsWriteRequest) {
          state.putPartitions((PartitionsWriteRequest) request);
        } else if (request instanceof AllVerticesRequest) {
          VerticesReadResponse response = state.getAllVertices();
          output.writeObject(response);
          output.flush();
        } else if (request instanceof CloseSessionRequest) {
          break;
        } else if (request instanceof ClearAllRequest) {
          state.releaseResources();
        } else {
          throw new ClassNotFoundException(request.getClass().toString());
        }
      }
    } catch (IOException | ClassNotFoundException ex) {
      ex.printStackTrace();
    } finally {
      try {
        socket.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }

  }
}
