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
      "-w", "2", "5", "5",
      "-p", "16",
      "-t", "2", "3", "3",
      //            "-reset", "true",
      "-s", "memory",
      "-db", "localhost:4444",
      "-user", "root",
      "-pass", "",
      "-output", "/Users/Ganymedian/Desktop/results/hovercut",
      "-append", "false", //FIXME: if appends is true it throws exception.
      "-d", "\" \"",
      "-puf", "1",
      "-rs", "0", "2",
      "-shuffle", "true",
      "-sg", "false",
      "-pg", "false",
      "-ed", "false",
      "-ne", "1",
      "-single", "false"};

    GraphPartitioner.main(args);
  }

}
