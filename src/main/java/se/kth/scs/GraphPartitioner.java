package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.PartitionsStatistics;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfInMemoryState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfMysqlState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfPartitioner;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfRemoteState;
import utils.EdgeFileReader;
import utils.PartitionerInputCommands;
import utils.PartitionerSettings;

/**
 *
 * @author Hooman
 */
public class GraphPartitioner {

  private static final Map<Integer, List<Float>> windowRf = new LinkedHashMap<>();
  private static final Map<Integer, List<Float>> taskRf = new LinkedHashMap<>();
  private static final Map<Integer, List<Float>> windowLb = new LinkedHashMap<>();
  private static final Map<Integer, List<Float>> taskLb = new LinkedHashMap<>();
  private static final Map<Integer, List<Integer>> windowTime = new LinkedHashMap<>();
  private static final Map<Integer, List<Integer>> taskTime = new LinkedHashMap<>();

  public static void main(String[] args) throws SQLException, IOException {
    PartitionerInputCommands commands = new PartitionerInputCommands();
    JCommander commander;
    try {
      commander = new JCommander(commands, args);
    } catch (ParameterException ex) {
      System.out.println(ex.getMessage());
      System.out.println(Arrays.toString(args));
      commander = new JCommander(commands);
      commander.usage();
      System.out.println(String.format("A valid command is like: %s",
        "-f ./data/datasets/Cit-HepTh.txt -w 10 0 5 -m hdrf -p 4 -t 2 0 6 -s remote -db localhost:4444"));
      System.exit(1);
    }
    if (!commands.method.equals(PartitionerInputCommands.HDRF)) {
      throw new ParameterException("");
    }

    PartitionerSettings settings = new PartitionerSettings();
    settings.k = (short) commands.nPartitions;
    settings.file = commands.file;
    settings.output = commands.output;
    settings.storage = commands.storage;
    settings.dbUrl = commands.dbUrl;
    settings.user = commands.user;
    settings.pass = commands.pass;
    settings.method = commands.method;
    settings.lambda = commands.lambda;
    settings.epsilon = commands.epsilon;
    settings.delay = commands.delay;
    if (settings.delay == null || settings.delay.size() != 2) {
      settings.delay = new ArrayList<>(2);
      settings.delay.add(0);
      settings.delay.add(0);
    }
    settings.append = commands.append;
    settings.reset = commands.reset;
    settings.delimiter = commands.delimiter;
    settings.frequency = commands.frequency;
    settings.restream = commands.restreaming;
    int wb = commands.window.get(0);
    int minW = commands.window.get(1);
    int maxW = commands.window.get(2);
    int tb = commands.nTasks.get(0);
    int minT = commands.nTasks.get(1);
    int maxT = commands.nTasks.get(2);

    for (int i = minT; i <= maxT; i++) {
      int t = (int) Math.pow(tb, i);
      int j = minW;
      int w;
      System.out.println(String.format("Reading file %s", settings.file));
      long start = System.currentTimeMillis();
      EdgeFileReader reader = new EdgeFileReader(settings.delimiter);
      LinkedHashSet<Edge>[] splits = reader.readSplitFile(settings.file, t);
      int nEdges = reader.getnEdges();
      System.out.println(String.format("Finished reading in %d seconds.", (System.currentTimeMillis() - start) / 1000));
      while (true) {
        w = (int) Math.pow(wb, j);
        if (w * t > nEdges) {
          break;
        } else if ((maxW >= 0) && (j > maxW)) {
          break;
        }
        settings.window = w;
        settings.tasks = t;

        runPartitioner(settings, splits);
        j++;
      }
    }
    writeToFile(settings);
  }

  private static void runPartitioner(PartitionerSettings settings, LinkedHashSet<Edge>[] splits) throws SQLException, IOException {
    printCommandSetup(settings);
    PartitionState state = null;
    LinkedList<Edge>[][] outputAssignments = null;
    PartitionsStatistics ps = null;
    long start = System.currentTimeMillis();
    for (int i = 0; i <= settings.restream; i++) {
      switch (settings.storage) {
        case PartitionerInputCommands.IN_MEMORY:
          state = new HdrfInMemoryState(settings.k);
          break;
        case PartitionerInputCommands.MYSQL:
          state = new HdrfMysqlState(
            settings.k,
            settings.dbUrl,
            settings.user,
            settings.pass,
            settings.reset);
          break;
        case PartitionerInputCommands.REMOTE:
          String[] url = settings.dbUrl.split(":");
          state = new HdrfRemoteState(settings.k, url[0], Integer.valueOf(url[1]));
          break;
        default:
          throw new ParameterException("");
      }

      if (outputAssignments != null) {
        for (int j = 0; j < splits.length; j++) {
          splits[j] = new LinkedHashSet();
          for (LinkedList l : outputAssignments[j]) {
            splits[j].addAll(l);
          }
        }
        outputAssignments = null;
      }

//      long start = System.currentTimeMillis();
      outputAssignments = HdrfPartitioner.partitionWithWindow(
        state,
        splits,
        settings.lambda,
        settings.epsilon,
        settings.window,
        settings.delay.get(0),
        settings.delay.get(1),
        settings.frequency);
      ps = new PartitionsStatistics(state);
      printResults(settings.k, ps, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", settings.lambda, settings.epsilon));
    }
    int duration = (int) ((System.currentTimeMillis() - start) / 1000);
    if (!settings.output.isEmpty()) {
      try {
        GraphPartitioner.writeToFile(ps, settings);
      } catch (FileNotFoundException ex) {
        ex.printStackTrace();
      }
    }

    if (!windowRf.containsKey(settings.window)) {
      windowRf.put(settings.window, new LinkedList<Float>());
    }
    windowRf.get(settings.window).add(ps.replicationFactor());
    if (!taskRf.containsKey(settings.tasks)) {
      taskRf.put(settings.tasks, new LinkedList<Float>());
    }
    taskRf.get(settings.tasks).add(ps.replicationFactor());
    if (!windowTime.containsKey(settings.window)) {
      windowTime.put(settings.window, new LinkedList<Integer>());
    }
    windowTime.get(settings.window).add(duration);
    if (!taskTime.containsKey(settings.tasks)) {
      taskTime.put(settings.tasks, new LinkedList<Integer>());
    }
    taskTime.get(settings.tasks).add(duration);
    if (!windowLb.containsKey(settings.window)) {
      windowLb.put(settings.window, new LinkedList<Float>());
    }
    windowLb.get(settings.window).add(ps.loadRelativeStandardDeviation());
    if (!taskLb.containsKey(settings.tasks)) {
      taskLb.put(settings.tasks, new LinkedList<Float>());
    }
    taskLb.get(settings.tasks).add(ps.loadRelativeStandardDeviation());
    state.releaseResources();
  }

  private static void printResults(int k, PartitionsStatistics ps, String message) {
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

  private static void printCommandSetup(PartitionerSettings settings) {
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

  public static void writeToFile(PartitionsStatistics ps, PartitionerSettings settings) throws FileNotFoundException {
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

  private static void writeToFile(
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
}
