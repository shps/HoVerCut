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
      "-p", "16",
      "-t", "2", "5", "5",
      //            "-reset", "true",
      "-s", "memory",
      "-db", "localhost:4444",
      "-user", "root",
      "-pass", "",
      "-output", "/home/ganymedian/Desktop/results/hdrf",
      "-append", "false", //FIXME: if appends is true it throws exception.
      "-delay", "0", "0",
      "-d", "\" \"",
      "-puf", "1",
      "-rs", "3",
      "-shuffle", "true",
      "-grouping", "true"};

    GraphPartitioner.main(args);
  }

}
