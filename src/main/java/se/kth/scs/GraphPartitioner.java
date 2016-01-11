package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfInMemoryState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfMysqlState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfPartitioner;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfRemoteState;
import utils.EdgeFileReader;
import utils.OutputManager;
import utils.PartitionerInputCommands;
import utils.PartitionerSettings;
import utils.PartitioningResult;
import utils.PartitionsStatistics;

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
    for (int i = 0; i < commands.numExperiments; i++) {
      runExperiments(commands, i);
    }
    output.writeToFile(commands.output, commands.append);
  }

  private static void runExperiments(PartitionerInputCommands commands, int num) throws IOException, Exception {
    int wb = commands.window.get(0);
    int minW = commands.window.get(1);
    int maxW = commands.window.get(2);
    int tb = commands.nTasks.get(0);
    int minT = commands.nTasks.get(1);
    int maxT = commands.nTasks.get(2);
    int minRestream = commands.restreaming.get(0);
    int maxRestream = commands.restreaming.get(1);
    PartitionerSettings settings = new PartitionerSettings(tb, minT, maxT, wb, minW, maxW);
    settings.setSettings(commands);

    System.out.println(String.format("Reading file %s", settings.file));
    long start = System.currentTimeMillis();
    long seed = start;
    EdgeFileReader reader = new EdgeFileReader(settings.delimiter);
    LinkedHashSet<Edge>[] splits = reader.readSplitFile(settings.file, 1, settings.shuffle, seed);
    System.out.println(String.format("Finished reading in %d seconds.", (System.currentTimeMillis() - start) / 1000));

//    if (settings.single) {
//      //TODO: add results
//      runSingleExperiment(settings, splits, reader.getnVertices());
//    }
    for (int i = minT; i <= maxT; i++) {
      int t = (int) Math.pow(tb, i);
      int j = minW;
      int w;
      System.out.println(String.format("Re-splitting file %s", settings.file));
      start = System.currentTimeMillis();
      splits = EdgeFileReader.resplit(splits, t, reader.getnEdges());
      int nEdges = reader.getnEdges();
      int nVertices = reader.getnVertices();
      System.out.println(String.format("Finished resplitting in %d seconds.", (System.currentTimeMillis() - start) / 1000));
      while (true) {
        w = (int) Math.pow(wb, j);
        if (w * t > nEdges) {
          break;
        } else if ((maxW >= 0) && (j > maxW)) {
          break;
        }
        settings.window = w;
        settings.tasks = t;
        for (int rs = minRestream; rs <= maxRestream; rs++) {
          settings.restream = rs;
          OutputManager.printCommandSetup(settings);
          start = System.currentTimeMillis();
          PartitionState state = runPartitioner(settings, splits, nVertices);
          int duration = (int) ((System.currentTimeMillis() - start) / 1000);
          PartitionsStatistics ps = new PartitionsStatistics(state, nVertices);
          OutputManager.printResults(settings.k, ps, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", settings.lambda, settings.epsilon));
          if (ps.getNVertices() != nVertices) {
            //To check the correctness.
            throw new Exception(String.format("Inconsistent number of vertices file=%d\tstorage=%d.", nVertices, ps.getNVertices()));
          }
          state.releaseResources(true);

          PartitioningResult result = new PartitioningResult(
            ps.replicationFactor(),
            ps.maxVertexCardinality(),
            ps.maxEdgeCardinality(),
            ps.loadRelativeStandardDeviation(),
            num,
            rs,
            settings.window,
            settings.tasks,
            seed,
            duration);
          output.addResult(result);
        }
        j++;
      }
    }
  }

  private static PartitionState runPartitioner(PartitionerSettings settings, LinkedHashSet<Edge>[] splits, int nVertices) throws SQLException, IOException, Exception {
    PartitionState state = null;
    if (settings.exactDegree) {
      System.out.println("Starts exact degree computation...");
      long edStart = System.currentTimeMillis();
      state = prepareState(settings, null, false);
      HdrfPartitioner.computeExactDegrees(state, splits);
      //TODO: This should be removed.
      state.waitForAllUpdates(nVertices);
      state.releaseTaskResources();
      long exactDegreeTime = (int) ((System.currentTimeMillis() - edStart) / 1000);
      System.out.println(String.format("******** Exact degree computation finished in %d seconds **********", exactDegreeTime));
    }
    LinkedList<Edge>[][] outputAssignments = null;
    boolean srcGrouping = settings.srcGrouping;
    boolean exactDegree = settings.exactDegree;
    for (int i = 0; i <= settings.restream; i++) {
      state = prepareState(settings, state, exactDegree);

      if (settings.pGrouping && outputAssignments != null) {
        for (int j = 0; j < splits.length; j++) {
          splits[j] = new LinkedHashSet();
          for (LinkedList l : outputAssignments[j]) {
            splits[j].addAll(l);
          }
        }
        outputAssignments = null;
      }
      outputAssignments = HdrfPartitioner.partitionWithWindow(
        state,
        splits,
        settings.lambda,
        settings.epsilon,
        settings.window,
        settings.delay.get(0),
        settings.delay.get(1),
        settings.frequency,
        srcGrouping,
        exactDegree);
      if (exactDegree == false && i == 0) {
        state.waitForAllUpdates(nVertices);
        state.releaseTaskResources();
      }
      exactDegree = true;
      srcGrouping = false;
    }

//    if (!settings.output.isEmpty()) {
//      try {
//        output.writeToFile(ps, settings);
//      } catch (FileNotFoundException ex) {
//        ex.printStackTrace();
//      }
//    }
    return state;
  }

  private static PartitionState prepareState(PartitionerSettings settings, PartitionState state, boolean exactDegree) throws SQLException, IOException {
    switch (settings.storage) {
      case PartitionerInputCommands.IN_MEMORY:
        if (state == null || !exactDegree) {
          state = new HdrfInMemoryState(settings.k);
        } else {
          state.releaseResources(false);
        }
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
        state = new HdrfRemoteState(settings.k, url[0], Integer.valueOf(url[1]), exactDegree);
        break;
      default:
        throw new ParameterException("");
    }

    return state;
  }

  private static void runSingleExperiment(PartitionerSettings settings, LinkedHashSet<Edge>[] splits, int nVertices) throws SQLException, IOException, Exception {
    PartitionerSettings singleSettings = new PartitionerSettings();
    singleSettings.setSettings(settings);
    singleSettings.srcGrouping = false;
    singleSettings.storage = PartitionerInputCommands.IN_MEMORY;
    singleSettings.window = 1;
    singleSettings.tasks = 1;
    singleSettings.frequency = 1;
    singleSettings.exactDegree = false;
    runPartitioner(singleSettings, splits, nVertices);
  }
}
