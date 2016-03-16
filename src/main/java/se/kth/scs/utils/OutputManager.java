package se.kth.scs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Hooman
 */
public class OutputManager {

  private final Map<PartitioningResult, List<PartitioningResult>> results = new LinkedHashMap<>();

  /**
   * Preservers order of the results.
   *
   * @param r
   */
  public void addResult(final PartitioningResult r) {
    List<PartitioningResult> list;
    if (results.containsKey(r)) {
      list = results.get(r);
    } else {
      list = new LinkedList<>();
      results.put(r, list);
    }
    list.add(r);
  }

  public void writeToFile(
    String output, boolean append) throws FileNotFoundException {
    String file1 = output + "-result.csv";
    String file2 = output + "-avg.csv";
    File f1 = new File(file1);
    File f2 = new File(file2);
    boolean shouldAppend = false;
    if (f1.exists() && !f1.isDirectory()) {
      shouldAppend = append;
    }
    try (PrintWriter writer1 = new PrintWriter(new FileOutputStream(f1, shouldAppend));
      PrintWriter writer2 = new PrintWriter(new FileOutputStream(f2, shouldAppend))) {
      if (!shouldAppend) {
        writer1.write("expriment,task,window,rs,seed,rf,lrsd,time,mvc,mec\n");
        writer2.write("task,window,rs,rf,lrsd,time,mvc,mec\n");
      }
      for (List<PartitioningResult> list : results.values()) {
        float rf = 0;
        float lrsd = 0;
        float time = 0;
        int mvc = 0;
        int mec = 0;
        for (PartitioningResult r : list) {
          writer1.append(String.format("%d,%d,%d,%f,%f,%f,%d,%d",
            r.task,
            r.window,
            r.seed,
            r.avgReplicationFactor,
            r.loadRelativeStandardDeviation,
            r.totalTime,
            r.maxVertexCardinality,
            r.maxEdgeCardinality));
          writer1.append("\n");
          rf += r.avgReplicationFactor;
          lrsd += r.loadRelativeStandardDeviation;
          time += r.totalTime;
          mvc += r.maxVertexCardinality;
          mec += r.maxEdgeCardinality;
        }
        int size = list.size();
        rf = rf / size;
        lrsd = lrsd / size;
        time = time / size;
        mvc = mvc / size;
        mec = mec / size;
        PartitioningResult r = list.get(0);
        writer2.append(String.format("%d,%d,%f,%f,%f,%d,%d",
          r.task,
          r.window,
          rf,
          lrsd,
          time,
          mvc,
          mec));
        writer2.append("\n");
      }
      writer1.flush();
      writer2.flush();
    }
  }

  public static void printResults(int k, PartitionsStatistics ps, String message) {
    System.out.println("*********** Statistics ***********");
    System.out.println(message);
    System.out.println(String.format("Partitions:\t%d", k));
    System.out.println(String.format("Vertices:\t%d", ps.getNVertices()));
    System.out.println(String.format("Edges:\t%d", ps.getNEdges()));
    int[] vp = ps.getNVerticesPartitions();
    int[] ep = ps.getNEdgesPartitions();
    for (int i = 0; i < vp.length; i++) {
      System.out.println(String.format("P%d:\tv=%d\te=%d", i, vp[i], ep[i]));
    }
    System.out.println("RF: Replication Factor.");
    System.out.println("LRSD: Load Relative Standard Deviation");
    System.out.println("MEC: Max Edge Cardinality.");
    System.out.println("MVC: Max Vertex Cardinality.");
    System.out.println(String.format("RF=%f\tLRSD=%f\tMEC=%d",
      ps.replicationFactor(),
      ps.loadRelativeStandardDeviation(),
      ps.maxEdgeCardinality(),
      ps.maxVertexCardinality()));
  }

  public static void printCommandSetup(PartitionerSettings settings) {
    final String newLine = "\n";
    StringBuilder sb = new StringBuilder("Your partitionig configurations:\n");
    sb.append("file:\t").append(settings.file).append(newLine);
    sb.append("window:\t").append(settings.window).append(newLine);
    sb.append("partitions update frequency:\t").append(settings.frequency).append(newLine);
    sb.append("partitions:\t").append(settings.k).append(newLine);
    sb.append("tasks(threads):\t").append(settings.tasks).append(newLine);
    sb.append("lambda:\t").append(settings.lambda).append(newLine);
    sb.append("epsilon:\t").append(settings.epsilon).append(newLine);
    sb.append("storage:\t").append(settings.storage).append(newLine);
    sb.append("reset storage:\t").append(settings.reset).append(newLine);
    if (settings.storage.contentEquals(PartitionerInputCommands.MYSQL)) {
      sb.append("db:\t").append(settings.dbUrl).append(newLine);
      sb.append("db user:\t").append(settings.user).append(newLine);
      sb.append("db pass:\t").append(settings.pass).append(newLine);
    }
    sb.append("output:\t").append(settings.output).append(newLine);
    sb.append("append to output:\t").append(settings.append).append(newLine);
    sb.append("shuffle input:\t").append(settings.shuffle).append(newLine);
    sb.append("compute exact degree:\t").append(settings.exactDegree).append(newLine);
    System.out.println(sb.toString());
  }
}
