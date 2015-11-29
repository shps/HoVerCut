package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;
import utils.PartitionsStatistics;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfInMemoryState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfMysqlState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfPartitioner;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfRemoteState;
import utils.EdgeFileReader;
import utils.OutputManager;
import utils.PartitionerInputCommands;
import utils.PartitionerSettings;

/**
 * This class is the main class to run the graph partitioning loader.
 *
 * @author Hooman
 */
public class GraphPartitioner {

  private static final OutputManager output = new OutputManager();

  public static void main(String[] args) throws SQLException, IOException, Exception {
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

    int wb = commands.window.get(0);
    int minW = commands.window.get(1);
    int maxW = commands.window.get(2);
    int tb = commands.nTasks.get(0);
    int minT = commands.nTasks.get(1);
    int maxT = commands.nTasks.get(2);
    PartitionerSettings settings = new PartitionerSettings(tb, minT, maxT, wb, minW, maxW);
    settings.setSettings(commands);

    if (settings.single) {
      runSingleExperiment(settings);
    }

    for (int i = minT; i <= maxT; i++) {
      int t = (int) Math.pow(tb, i);
      int j = minW;
      int w;
      System.out.println(String.format("Reading file %s", settings.file));
      long start = System.currentTimeMillis();
      EdgeFileReader reader = new EdgeFileReader(settings.delimiter);
      LinkedHashSet<Edge>[] splits = reader.readSplitFile(settings.file, t, settings.shuffle);
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

        runPartitioner(settings, splits, reader.getnVertices());
        j++;
      }
    }
    output.writeToFile(settings);
  }

  private static void runPartitioner(PartitionerSettings settings, LinkedHashSet<Edge>[] splits, int nVertices) throws SQLException, IOException, Exception {
    OutputManager.printCommandSetup(settings);
    PartitionState state = null;
    LinkedList<Edge>[][] outputAssignments = null;
    PartitionsStatistics ps = null;
    boolean srcGrouping = settings.srcGrouping;
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
        srcGrouping = false;
      }

      long start = System.currentTimeMillis();
      outputAssignments = HdrfPartitioner.partitionWithWindow(
        state,
        splits,
        settings.lambda,
        settings.epsilon,
        settings.window,
        settings.delay.get(0),
        settings.delay.get(1),
        settings.frequency,
        srcGrouping);
      int duration = (int) ((System.currentTimeMillis() - start) / 1000);
      ps = new PartitionsStatistics(state, nVertices);
      output.addResults(settings.window, settings.tasks, duration, ps.replicationFactor(), ps.loadRelativeStandardDeviation());
      OutputManager.printResults(settings.k, ps, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", settings.lambda, settings.epsilon));
      if (ps.getNVertices() != nVertices) {
        throw new Exception(String.format("Inconsistent number of vertices file=%d\tstorage=%d.", nVertices, ps.getNVertices()));
      }
    }
    if (!settings.output.isEmpty()) {
      try {
        output.writeToFile(ps, settings);
      } catch (FileNotFoundException ex) {
        ex.printStackTrace();
      }
    }

    state.releaseResources();
  }

  private static void runSingleExperiment(PartitionerSettings settings) throws SQLException, IOException, Exception {
    PartitionerSettings singleSettings = new PartitionerSettings();
    singleSettings.setSettings(settings);
    singleSettings.srcGrouping = false;
    singleSettings.storage = PartitionerInputCommands.IN_MEMORY;
    singleSettings.window = 1;
    singleSettings.tasks = 1;
    singleSettings.frequency = 1;
    System.out.println(String.format("Reading file %s", singleSettings.file));
    long start = System.currentTimeMillis();
    EdgeFileReader reader = new EdgeFileReader(singleSettings.delimiter);
    LinkedHashSet<Edge>[] splits = reader.readSplitFile(singleSettings.file, singleSettings.tasks, singleSettings.shuffle);
    System.out.println(String.format("Finished reading in %d seconds.", (System.currentTimeMillis() - start) / 1000));
    runPartitioner(singleSettings, splits, reader.getnVertices());
  }
}
