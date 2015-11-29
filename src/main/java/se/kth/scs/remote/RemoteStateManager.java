package se.kth.scs.remote;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import utils.StorageInputCommands;

/**
 * The main class to run the remote state storage. The remote state storage,
 * is a fast and efficient storage, with customized serializations for communication
 * with the clients.
 * 
 * @author Hooman
 */
public class RemoteStateManager {

  public static void main(String[] args) {
    StorageInputCommands commands = new StorageInputCommands();
    JCommander commander;
    try {
      commander = new JCommander(commands, args);
    } catch (ParameterException ex) {
      System.out.println(ex.getMessage());
      System.out.println(Arrays.toString(args));
      commander = new JCommander(commands);
      commander.usage();
      System.out.println(String.format("A valid command is like: %s",
          "-p 4 -a localhost:4444"));
      System.exit(1);
    }

    String[] addr = commands.address.split(":");
    try (ServerSocket server = new ServerSocket(Integer.valueOf(addr[1]), 0, InetAddress.getByName(addr[0]))) {
      ServerStorage state = new ServerStorage((short) commands.nPartitions);
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
