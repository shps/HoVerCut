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
      "-a", "greedy",
      "-w", "10", "0", "0",
      "-p", "16",
      "-t", "2", "0", "0",
      //            "-reset", "true",
      "-s", "memory",
      "-db", "localhost:4444",
      "-user", "root",
      "-pass", "",
      "-output", "/Users/Ganymedian/Desktop/results/hovercut",
      "-append", "false", //FIXME: if appends is true it throws exception.
      "-d", "\" \"",
      "-puf", "1",
      "-rs", "0", "0",
      "-shuffle", "true",
      "-sg", "false",
      "-pg", "false",
      "-ed", "false",
      "-ne", "1",
      "-single", "false"};

    GraphPartitioner.main(args);
  }

}
