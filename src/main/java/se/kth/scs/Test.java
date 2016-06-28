package se.kth.scs;


import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Hooman
 */
public class Test {

  public static void main(String[] args) throws SQLException, IOException, Exception {
    args = new String[]{
      "-f", "./data/datasets/twitter_combined.txt",
      "-a", "hdrf",
      "-w", "32",
      "-p", "16",
      "-t", "2",
      //            "-reset", "true",
      "-s", "memory",
      "-db", "localhost:4444",
      "-user", "root",
      "-pass", "",
//      "-output", "./results/hovercut",
//      "-append", "false", 
      "-rs", "0",
      "-d", "\" \"",
      "-puf", "1",
      "-ed", "false",
      "-shuffle", "true"};

    GraphPartitioner.main(args);
  }

}
