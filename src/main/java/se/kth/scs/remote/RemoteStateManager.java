package se.kth.scs.remote;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Hooman
 */
public class RemoteStateManager {

  private final static int k = 4;
  private final static int port = 4444;
  private final static String address = "127.0.0.1";

  public static void main(String[] args) {
    try {
      ServerSocket server = new ServerSocket(port, 0, InetAddress.getByName(address));
      ServerStorage state = new ServerStorage(k);
      System.out.println("Server is waiting for clients to connect...");
      int i = 1;
      while (true) {
        Socket socket = server.accept();
        System.out.println(String.format("Received connection request from client %d", i));
        Thread t = new Thread(new QueryHandler(state, socket));
        // We can postpone starting to when we receive all the connections.
        t.start();
        i++;
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

}
