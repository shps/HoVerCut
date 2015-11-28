/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.scs;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Hooman
 */
public class Test {

  public static void main(String[] args) throws SQLException, IOException {
    args = new String[]{
      "-f", "./data/datasets/twitter_combined.txt",
      "-w", "10", "3", "3",
      "-m", "hdrf",
      "-p", "4",
      "-t", "2", "3", "3",
      //            "-reset", "true",
      "-s", "remote",
      "-db", "localhost:4444",
      "-user", "root",
      "-pass", "",
      "-output", "/home/ganymedian/Desktop/results/hdrf",
      "-append", "true", //FIXME: if appends is true it throws exception.
      "-d", "\" \"",
      "-puf", "1",
      "-rs", "1",
      "-shuffle", "false",
      "-grouping", "false",
      "-single", "false"};

    GraphPartitioner.main(args);
  }

}
