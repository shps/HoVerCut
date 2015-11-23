package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.kth.scs.GraphPartitioner;
import se.kth.scs.partitioning.PartitionsStatistics;

/**
 *
 * @author Hooman
 */
public class OutputManager {

  private final Map<Integer, List<Float>> windowRf = new LinkedHashMap<>();
  private final Map<Integer, List<Float>> taskRf = new LinkedHashMap<>();
  private final Map<Integer, List<Float>> windowLb = new LinkedHashMap<>();
  private final Map<Integer, List<Float>> taskLb = new LinkedHashMap<>();
  private final Map<Integer, List<Integer>> windowTime = new LinkedHashMap<>();
  private final Map<Integer, List<Integer>> taskTime = new LinkedHashMap<>();

  /**
   *
   * @param w window size
   * @param t number of tasks
   * @param d duration
   * @param rf replication factor
   * @param lrsd load relative standard deviation
   */
  public void addResults(int w, int t, int d, float rf, float lrsd) {
    addRfToWindow(rf, w);
    addRfToTask(rf, t);
    addDurationToWindow(d, w);
    addDurationToTask(d, t);
    addLrsdToWindow(lrsd, w);
    addLrsdToTask(lrsd, t);
  }

  public void addRfToWindow(float rf, int w) {
    if (!windowRf.containsKey(w)) {
      windowRf.put(w, new LinkedList<Float>());
    }
    windowRf.get(w).add(rf);
  }

  public void addRfToTask(float rf, int t) {
    if (!taskRf.containsKey(t)) {
      taskRf.put(t, new LinkedList<Float>());
    }

    taskRf.get(t).add(rf);
  }

  public void addDurationToWindow(int d, int w) {
    if (!windowTime.containsKey(w)) {
      windowTime.put(w, new LinkedList<Integer>());
    }

    windowTime.get(w).add(d);
  }

  public void addDurationToTask(int d, int t) {
    if (!taskTime.containsKey(t)) {
      taskTime.put(t, new LinkedList<Integer>());
    }

    taskTime.get(t).add(d);
  }

  public void addLrsdToWindow(float l, int w) {
    if (!windowLb.containsKey(w)) {
      windowLb.put(w, new LinkedList<Float>());
    }

    windowLb.get(w).add(l);
  }

  public void addLrsdToTask(float l, int t) {
    if (!taskLb.containsKey(t)) {
      taskLb.put(t, new LinkedList<Float>());
    }

    taskLb.get(t).add(l);
  }

  public void writeToFile(PartitionsStatistics ps, PartitionerSettings settings) throws FileNotFoundException {
//        File f1 = new File(commands.output + "-partitions.csv");
//        boolean append = false;
//        if (f1.exists() && !f1.isDirectory()) {
//            append = commands.append;
//        }
//        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
//                f1,
//                append))) {
//            Collection<Vertex> vertices = ps.getVertices().values();
//            for (Vertex v : vertices) {
//                writer.append(String.format("%d,", v.getId()));
//                for (int p : v.getPartitions()) {
//                    writer.append(String.format("%d,", p));
//                }
//                writer.append("\n");
//            }
//
//            writer.flush();
//        }
    boolean append = false;
    String file = settings.output + "-result.csv";
    File f2 = new File(file);
    if (f2.exists() && !f2.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f2,
      append))) {
      if (!append) {
        writer.write("nTasks,nPartitions,window,rf,lrsd,mec,mvc\n");
      }
      writer.append(String.format("%d,%d,%d,%f,%f,%d,%d",
        settings.tasks,
        settings.k,
        settings.window,
        ps.replicationFactor(),
        ps.loadRelativeStandardDeviation(),
        ps.maxEdgeCardinality(),
        ps.maxVertexCardinality()));
      writer.append("\n");
      writer.flush();
    }
  }

  public void writeToFile(
    PartitionerSettings settings) {
    boolean append = false;
    String fName = settings.output + "-window.csv";
    File f1 = new File(fName);
    if (f1.exists() && !f1.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f1,
      append))) {
      Iterator<Map.Entry<Integer, List<Float>>> iterator = windowRf.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, List<Float>> entry = iterator.next();
        writer.append(entry.getKey().toString()).append(",");
        for (float rf : entry.getValue()) {
          writer.append(String.valueOf(rf)).append(",");
        }
        writer.append("\n");
      }

      writer.flush();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
    }

    append = false;
    fName = settings.output + "-tasks.csv";
    f1 = new File(fName);
    if (f1.exists() && !f1.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f1,
      append))) {
      Iterator<Map.Entry<Integer, List<Float>>> iterator = taskRf.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, List<Float>> entry = iterator.next();
        writer.append(entry.getKey().toString()).append(",");
        for (float rf : entry.getValue()) {
          writer.append(String.valueOf(rf)).append(",");
        }
        writer.append("\n");
      }

      writer.flush();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
    }

    append = false;
    fName = settings.output + "-tasks-lb.csv";
    f1 = new File(fName);
    if (f1.exists() && !f1.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f1,
      append))) {
      Iterator<Map.Entry<Integer, List<Float>>> iterator = taskLb.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, List<Float>> entry = iterator.next();
        writer.append(entry.getKey().toString()).append(",");
        for (float lb : entry.getValue()) {
          writer.append(String.valueOf(lb)).append(",");
        }
        writer.append("\n");
      }

      writer.flush();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
    }

    append = false;
    fName = settings.output + "-windows-lb.csv";
    f1 = new File(fName);
    if (f1.exists() && !f1.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f1,
      append))) {
      Iterator<Map.Entry<Integer, List<Float>>> iterator = windowLb.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, List<Float>> entry = iterator.next();
        writer.append(entry.getKey().toString()).append(",");
        for (float lb : entry.getValue()) {
          writer.append(String.valueOf(lb)).append(",");
        }
        writer.append("\n");
      }

      writer.flush();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
    }

    append = false;
    fName = settings.output + "-windows-time.csv";
    f1 = new File(fName);
    if (f1.exists() && !f1.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f1,
      append))) {
      Iterator<Map.Entry<Integer, List<Integer>>> iterator = windowTime.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, List<Integer>> entry = iterator.next();
        writer.append(entry.getKey().toString()).append(",");
        for (int time : entry.getValue()) {
          writer.append(String.valueOf(time)).append(",");
        }
        writer.append("\n");
      }

      writer.flush();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
    }

    append = false;
    fName = settings.output + "-tasks-time.csv";
    f1 = new File(fName);
    if (f1.exists() && !f1.isDirectory()) {
      append = settings.append;
    }
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(
      f1,
      append))) {
      Iterator<Map.Entry<Integer, List<Integer>>> iterator = taskTime.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, List<Integer>> entry = iterator.next();
        writer.append(entry.getKey().toString()).append(",");
        for (int time : entry.getValue()) {
          writer.append(String.valueOf(time)).append(",");
        }
        writer.append("\n");
      }

      writer.flush();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
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
    sb.append("Output:\t").append(settings.output).append(newLine);
    sb.append(String.format("Delay:\t min:%d\tmax=%d", settings.delay.get(0), settings.delay.get(1))).append(newLine);
    sb.append("Append to output:\t").append(settings.append).append(newLine);
    System.out.println(sb.toString());
  }
}
