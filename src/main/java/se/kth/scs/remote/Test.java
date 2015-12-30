package se.kth.scs.remote;

/**
 *
 * @author Hooman
 */
public class Test {

  public static void main(String[] args) {
    args = new String[]{
      "-p", "16",
      "-a", "localhost:4444"};
    RemoteStateManager.main(args);
  }

}
