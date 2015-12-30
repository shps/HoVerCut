package se.kth.scs.remote.messages;

/**
 * Includes all the message types that can be communicated between the client
 * and the remote state storage.
 *
 * @author Hooman
 */
public class Protocol {

  public final static byte ALL_VERTICES_REQUEST = 0;
  public final static byte CLEAR_ALL_REQUEST = 1;
  public final static byte CLOSE_SESSION_REQUEST = 2;
  public final static byte PARTITIONS_REQUEST = 3;
  public final static byte PARTITIONS_RESPONSE = 4;
  public final static byte PARTITIONS_WRITE_REQUEST = 5;
  public final static byte VERTICES_READ_REQUEST = 6;
  public final static byte VERTICES_READ_RESPONSE = 7;
  public final static byte VERTICES_WRITE_REQUEST = 8;
  public final static byte CLEAR_ALL_BUT_DEGREE_REQUEST = 9;
  public final static byte WAIT_FOR_ALL_UPDATES_REQUEST = 10;
  public final static byte WAIT_FOR_ALL_UPDATES_RESPONSE = 11;

}
