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
      "-w", "10", "3", "3",
      "-m", "hdrf",
      "-p", "4",
      "-t", "2", "2", "3",
      //            "-reset", "true",
      "-s", "memory",
      "-db", "localhost:4444",
      "-user", "root",
      "-pass", "",
      "-output", "/Users/Ganymedian/Desktop/results/hdrf",
      "-append", "true", //FIXME: if appends is true it throws exception.
      "-d", "\" \"",
      "-puf", "1",
      "-rs", "0",
      "-shuffle", "true",
      "-sg", "false",
      "-pg", "false",
      "-single", "false"};

    GraphPartitioner.main(args);
  }

}
