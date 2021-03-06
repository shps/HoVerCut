package se.kth.scs.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import se.kth.scs.partitioning.Edge;

/**
 * A Utility to read graph edges separated with a specific delimiters.
 *
 * @author Hooman
 */
public class EdgeFileReader {

  private final String DEFAULT_DELIMITER = "\t";
  private final String COMMENT = "#";
  private final String delimiter;
  private int nEdges;
  private int nVertices;

  public EdgeFileReader() {
    this.delimiter = DEFAULT_DELIMITER;
  }

  public EdgeFileReader(String delimiter) {
    this.delimiter = delimiter;
  }

  public LinkedHashSet<Edge> read(String file) {
    HashSet<Integer> vertices = new HashSet<>();
    LinkedHashSet<Edge> edges = new LinkedHashSet<>();
//    HashMap<Integer, Integer> degrees = new HashMap<>();
    try {
      FileInputStream fis = new FileInputStream(new File(file));
      InputStreamReader isr = new InputStreamReader(fis);
      try (BufferedReader in = new BufferedReader(isr)) {
        String line;
        while ((line = in.readLine()) != null) {
          if (line.startsWith(COMMENT)) {
            continue;
          } //skip comments
          String values[] = line.split(delimiter);
          int u = Integer.parseInt(values[0]);
          int v = Integer.parseInt(values[1]);
          if (u != v) {
            Edge e = new Edge(u, v);

            if (edges.add(e)) {
              nEdges++;
            }

            if (vertices.add(u)) {
              nVertices++;
            }
            if (vertices.add(v)) {
              nVertices++;
            }
//
//            if (!degrees.containsKey(u)) {
//              degrees.put(u, 0);
//            }
//            degrees.put(u, degrees.get(u) + 1);
//            if (!degrees.containsKey(v)) {
//              degrees.put(v, 0);
//            }
//            degrees.put(v, degrees.get(v) + 1);
          }
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
      System.exit(-1);
    }

    System.out.println(String.format("Number of vertices: %d", nVertices));
    System.out.println(String.format("Number of edges: %d", nEdges));

    return edges;
  }

  public LinkedHashSet[] readSplitFile(String file, int nSplit, boolean shuffle, long seed) {
    LinkedHashSet<Edge> allEdges = read(file);
    nEdges = allEdges.size();
    LinkedHashSet<Edge>[] splits = new LinkedHashSet[nSplit];
    int splitSize = allEdges.size() / nSplit + 1;
    Iterator<Edge> it;
    if (shuffle) {
      List<Edge> shuffled = new ArrayList<>(allEdges);
      Collections.shuffle(shuffled, new Random(seed));
      it = shuffled.iterator();
    } else {
      it = allEdges.iterator();
    }
    int j = -1;
    int i = 0;
    while (it.hasNext()) {
      Edge e = it.next();
      if (i % splitSize == 0) {
        j++;
        splits[j] = new LinkedHashSet<>();
      }
      splits[j].add(e);
      i++;
    }
    return splits;
  }

  public static LinkedHashSet<Edge>[] resplit(LinkedHashSet<Edge>[] splits, int nSplit, int eSize) {
    LinkedHashSet<Edge>[] newSplits = new LinkedHashSet[nSplit];
    int splitSize = eSize / nSplit + 1;
    int j = -1;
    int i = 0;
    for (LinkedHashSet<Edge> l : splits) {
      for (Edge e : l) {
        if (i % splitSize == 0) {
          j++;
          newSplits[j] = new LinkedHashSet<>();
        }
        newSplits[j].add(e);
        i++;
      }
    }

    return newSplits;
  }

  /**
   * @return the nEdges
   */
  public int getnEdges() {
    return nEdges;
  }

  public int getnVertices() {
    return nVertices;
  }
}
