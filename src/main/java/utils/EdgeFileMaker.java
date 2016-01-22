package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 *
 * @author Hooman
 */
public class EdgeFileMaker {

  private static final String INPUT_DELIMITER = " ";
  private static final String OUTPUT_DELIMITER = "\t";

  public static void main(String args[]) throws FileNotFoundException, IOException {
    String input = args[0];
    String output = args[1];

//    String input = "/home/ganymedian/Desktop/kdd-results/samples/g220";
//    String output = "/home/ganymedian/Desktop/kdd-results/samples/graph220.txt";
    InputStreamReader isr;
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(new File(output)))) {
      FileInputStream fis = new FileInputStream(new File(input));
      isr = new InputStreamReader(fis);
      BufferedReader in = new BufferedReader(isr);
      String line;
      while ((line = in.readLine()) != null) {
        String[] vertices = line.split(INPUT_DELIMITER);
        String src = vertices[0];
        for (int i = 1; i < vertices.length; i++) {
          StringBuilder sb = new StringBuilder();
          sb.append(src).append(OUTPUT_DELIMITER).append(vertices[i]);
          writer.println(sb.toString());
        }
      }
      writer.flush();
    }
    isr.close();
  }
}
