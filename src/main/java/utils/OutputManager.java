package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Hooman
 */
public class OutputManager {

//  private final Map<Integer, List<Float>> windowRf = new LinkedHashMap<>();
//  private final Map<Integer, Map<Integer, List<Number>>> windowRf = new LinkedHashMap<>();
//  private final Map<Integer, Map<Integer, List<Number>>> taskRf = new LinkedHashMap<>();
//  private final Map<Integer, Map<Integer, List<Number>>> windowLb = new LinkedHashMap<>();
//  private final Map<Integer, Map<Integer, List<Number>>> taskLb = new LinkedHashMap<>();
//  private final Map<Integer, Map<Integer, List<Number>>> windowTime = new LinkedHashMap<>();
//  private final Map<Integer, Map<Integer, List<Number>>> taskTime = new LinkedHashMap<>();
  private final List<PartitioningResult> results = new LinkedList<>();

//  private final List<Long> seeds = new LinkedList<>();

//  public void addSeed(long seed) {
//    seeds.add(seed);
//  }

  /**
   * Preservers order of the results.
   *
   * @param r
   */
  public void addResult(final PartitioningResult r) {
    results.add(r);
  }

  public void writeToFile(
    String output, boolean append) throws FileNotFoundException {
    String file = output + "-result.csv";
    File f2 = new File(file);
    boolean shouldAppend = false;
    if (f2.exists() && !f2.isDirectory()) {
      shouldAppend = append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f2,
      shouldAppend))) {
      if (!shouldAppend) {
        writer.write("expriment,task,window,rs,seed,rf,lrsd,time,mvc,mec\n");
      }
      for (PartitioningResult r : results) {
        writer.append(String.format("%d,%d,%d,%d,%d,%f,%f,%d,%d,%d",
          r.experimentNumber,
          r.task,
          r.window,
          r.restreaming,
          r.seed,
          r.avgReplicationFactor,
          r.loadRelativeStandardDeviation,
          r.totalTime,
          r.maxVertexCardinality,
          r.maxEdgeCardinality));
        writer.append("\n");
      }
      writer.flush();
    }

  }

//  /**
//   *
//   * @param w window size
//   * @param t number of tasks
//   * @param d duration
//   * @param rf replication factor
//   * @param lrsd load relative standard deviation
//   */
//  public void addResults(int w, int t, int d, float rf, float lrsd) {
//    addRfToWindow(rf, w, t);
//    addRfToTask(rf, t, w);
//    addDurationToWindow(d, w, t);
//    addDurationToTask(d, t, w);
//    addLrsdToWindow(lrsd, w, t);
//    addLrsdToTask(lrsd, t, w);
//  }
//
//  public void addRfToWindow(float rf, int w, int t) {
//    checkEntries(w, t, windowRf);
//    windowRf.get(w).get(t).add(rf);
//  }
//
//  private <F, S, T> Map<F, Map<S, List<T>>> checkEntries(F first, S second, Map<F, Map<S, List<T>>> map) {
//    if (!map.containsKey(first)) {
//      map.put(first, new LinkedHashMap<S, List<T>>());
//    }
//    if (!map.get(first).containsKey(second)) {
//      map.get(first).put(second, new LinkedList<T>());
//    }
//
//    return map;
//  }
//
//  public void addRfToTask(float rf, int t, int w) {
//    checkEntries(t, w, taskRf);
//    taskRf.get(t).get(w).add(rf);
//  }
//
//  public void addDurationToWindow(int d, int w, int t) {
//    checkEntries(w, t, windowTime);
//    windowTime.get(w).get(t).add(d);
//  }
//
//  public void addDurationToTask(int d, int t, int w) {
//    checkEntries(t, w, taskTime);
//    taskTime.get(t).get(w).add(d);
//  }
//
//  public void addLrsdToWindow(float l, int w, int t) {
//    checkEntries(w, t, windowLb);
//    windowLb.get(w).get(t).add(l);
//  }
//
//  public void addLrsdToTask(float l, int t, int w) {
//    checkEntries(t, w, taskLb);
//    taskLb.get(t).get(w).add(l);
//  }
//
//  public void writeToFile(PartitionsStatistics ps, PartitionerSettings settings) throws FileNotFoundException {
//    boolean shouldAppend = false;
//    String file = settings.output + "-result.csv";
//    File f2 = new File(file);
//    if (f2.exists() && !f2.isDirectory()) {
//      shouldAppend = settings.shouldAppend;
//    }
//    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
//      f2,
//      shouldAppend))) {
//      if (!shouldAppend) {
//        writer.write("nTasks,nPartitions,window,rf,lrsd,mec,mvc\n");
//      }
//      writer.shouldAppend(String.format("%d,%d,%d,%f,%f,%d,%d",
//        settings.tasks,
//        settings.k,
//        settings.window,
//        ps.replicationFactor(),
//        ps.loadRelativeStandardDeviation(),
//        ps.maxEdgeCardinality(),
//        ps.maxVertexCardinality()));
//      writer.shouldAppend("\n");
//      writer.flush();
//    }
//  }
//  public void writeToFile(
//    PartitionerSettings settings) {
//    boolean isWindow = true;
//    String fName = settings.output + "-window.csv";
//    writeWindowTaskToFile(fName, settings, windowRf, isWindow);
//    fName = settings.output + "-windows-lb.csv";
//    writeWindowTaskToFile(fName, settings, windowLb, isWindow);
//    fName = settings.output + "-windows-time.csv";
//    writeWindowTaskToFile(fName, settings, windowTime, isWindow);
//
//    isWindow = false;
//    fName = settings.output + "-tasks.csv";
//    writeWindowTaskToFile(fName, settings, taskRf, isWindow);
//    fName = settings.output + "-tasks-lb.csv";
//    writeWindowTaskToFile(fName, settings, taskLb, isWindow);
//    fName = settings.output + "-tasks-time.csv";
//    writeWindowTaskToFile(fName, settings, taskTime, isWindow);
//
//  }
//  /**
//   *
//   * @param fName
//   * @param settings
//   * @param result
//   * @param isWindow
//   */
//  private void writeWindowTaskToFile(String fName, PartitionerSettings settings, Map<Integer, Map<Integer, List<Number>>> result, boolean isWindow) {
//    boolean shouldAppend = false;
//    File f1 = new File(fName);
//    if (f1.exists() && !f1.isDirectory()) {
//      shouldAppend = settings.shouldAppend;
//    }
//    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
//      f1,
//      shouldAppend))) {
//      if (!shouldAppend) {
//        String firstHeader;
//        String secondHeader;
//        int base;
//        int minP;
//        int maxP;
//        if (isWindow) {
//          firstHeader = "W,";
//          secondHeader = "T";
//          base = settings.tb;
//          minP = settings.minT;
//          maxP = settings.maxT;
//        } else {
//          firstHeader = "T,";
//          secondHeader = "W";
//          base = settings.wb;
//          minP = settings.minW;
//          maxP = settings.maxW;
//        }
//        writer.write(firstHeader);
//        if (settings.single) {
//          int taskId = 1;
//          for (int j = 0; j <= settings.restream; j++) {
//            writer.shouldAppend(String.format("%s%d-%d,", secondHeader, taskId, j));
//          }
//        }
//        for (int i = minP; i <= maxP; i++) {
//          int taskId = (int) Math.pow(base, i);
//          if (settings.exactDegree) {
//            writer.shouldAppend(String.format("%s%d-%s,", secondHeader, taskId, "ed"));
//          }
//          for (int j = 0; j <= settings.restream; j++) {
//            writer.shouldAppend(String.format("%s%d-%d,", secondHeader, taskId, j));
//          }
//        }
//        writer.println();
//      }
//      StringBuilder sd = new StringBuilder();
//      if (settings.single) {
//        for (int i = 0; i <= settings.restream; i++) {
//          sd.shouldAppend(",");
//        }
//      }
//      boolean firstRound = true;
//      Iterator<Map.Entry<Integer, Map<Integer, List<Number>>>> it1 = result.entrySet().iterator();
//      while (it1.hasNext()) {
//        Map.Entry<Integer, Map<Integer, List<Number>>> entry = it1.next();
//        writer.shouldAppend(entry.getKey().toString()).shouldAppend(",");
//        Iterator<Map.Entry<Integer, List<Number>>> it2 = entry.getValue().entrySet().iterator();
//        if (!firstRound) {
//          writer.shouldAppend(sd.toString());
//        }
//        firstRound = false;
//        while (it2.hasNext()) {
//          Map.Entry<Integer, List<Number>> entry2 = it2.next();
//          for (Number rf : entry2.getValue()) {
//            writer.shouldAppend(String.valueOf(rf)).shouldAppend(",");
//          }
//        }
//        writer.shouldAppend("\n");
//      }
//
//      writer.flush();
//    } catch (FileNotFoundException ex) {
//      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
//    }
//  }
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
    sb.append("number of restreaming:\t").append(settings.restream).append(newLine);
    sb.append("method:\t").append(settings.method).append(newLine);
    sb.append("partitions:\t").append(settings.k).append(newLine);
    sb.append("tasks(threads):\t").append(settings.tasks).append(newLine);

    if (settings.storage.contentEquals(PartitionerInputCommands.HDRF)) {
      sb.append("lambda:\t").append(settings.lambda).append(newLine);
      sb.append("epsilon:\t").append(settings.epsilon).append(newLine);
    }
    sb.append("storage:\t").append(settings.storage).append(newLine);
    sb.append("reset storage:\t").append(settings.reset).append(newLine);
    if (settings.storage.contentEquals(PartitionerInputCommands.MYSQL)) {
      sb.append("db:\t").append(settings.dbUrl).append(newLine);
      sb.append("db user:\t").append(settings.user).append(newLine);
      sb.append("db pass:\t").append(settings.pass).append(newLine);
    }
    sb.append("output:\t").append(settings.output).append(newLine);
    sb.append(String.format("Delay:\t min:%d\tmax=%d", settings.delay.get(0), settings.delay.get(1))).append(newLine);
    sb.append("append to output:\t").append(settings.append).append(newLine);
    sb.append("shuffle input:\t").append(settings.shuffle).append(newLine);
    sb.append("source grouping:\t").append(settings.srcGrouping).append(newLine);
    sb.append("partition grouping:\t").append(settings.pGrouping).append(newLine);
    sb.append("compute exact degree:\t").append(settings.exactDegree).append(newLine);
    sb.append("+single experiment:\t").append(settings.single).append(newLine);
    System.out.println(sb.toString());
  }
}
