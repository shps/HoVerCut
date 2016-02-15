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
import se.kth.scs.partitioning.heuristics.Hdrf;
import se.kth.scs.partitioning.heuristics.Greedy;
import se.kth.scs.partitioning.heuristics.Heuristic;
import se.kth.scs.partitioning.hovercut.HovercutInMemoryState;
import se.kth.scs.partitioning.hovercut.HovercutMysqlState;
import se.kth.scs.partitioning.hovercut.HovercutPartitioner;
import se.kth.scs.partitioning.hovercut.HovercutRemoteState;
import se.kth.scs.utils.EdgeFileReader;
import se.kth.scs.utils.OutputManager;
import se.kth.scs.utils.PartitionerInputCommands;
import se.kth.scs.utils.PartitionerSettings;
import se.kth.scs.utils.PartitioningResult;
import se.kth.scs.utils.PartitionsStatistics;

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
        "-f ./data/datasets/Cit-HepTh.txt -w 10 0 5 -p 4 -t 2 0 6 -s remote -db localhost:4444"));
      System.exit(1);
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
          float duration = (float) (System.currentTimeMillis() - start) / (float) 1000;
          PartitionsStatistics ps = new PartitionsStatistics(state, nVertices);
          String message = null;
          if (settings.algorithm.equalsIgnoreCase(PartitionerInputCommands.HDRF)) {
            message = String.format("HoVerCut %s, lambda=%f\tepsilon=%f", settings.algorithm, settings.lambda, settings.epsilon);
          } else if (settings.algorithm.equalsIgnoreCase(PartitionerInputCommands.GREEDY)) {
            message = String.format("HoVerCut %s, epsilon=%f", settings.algorithm, settings.epsilon);
          }
          OutputManager.printResults(settings.k, ps, message);
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
      HovercutPartitioner.computeExactDegrees(state, splits);
      //TODO: This should be removed.
      state.waitForAllUpdates(nVertices);
      state.releaseTaskResources();
      long exactDegreeTime = (int) ((System.currentTimeMillis() - edStart) / 1000);
      System.out.println(String.format("******** Exact degree computation finished in %d seconds **********", exactDegreeTime));
    }
    LinkedList<Edge>[][] outputAssignments = null;
    boolean srcGrouping = settings.srcGrouping;
    boolean exactDegree = settings.exactDegree;
    Heuristic heuristic = buildHeuristic(settings);
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
      outputAssignments = HovercutPartitioner.partitionWithWindow(
        state,
        splits,
        heuristic,
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
          state = new HovercutInMemoryState(settings.k);
        } else {
          state.releaseResources(false);
        }
        break;
      case PartitionerInputCommands.MYSQL:
        state = new HovercutMysqlState(
          settings.k,
          settings.dbUrl,
          settings.user,
          settings.pass,
          settings.reset);
        break;
      case PartitionerInputCommands.REMOTE:
        String[] url = settings.dbUrl.split(":");
        state = new HovercutRemoteState(settings.k, url[0], Integer.valueOf(url[1]), exactDegree);
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

  private static Heuristic buildHeuristic(PartitionerSettings settings) {
    Heuristic h = null;
    if (settings.algorithm.equalsIgnoreCase(PartitionerInputCommands.HDRF)) {
      h = new Hdrf(settings.lambda, settings.epsilon);
    } else if (settings.algorithm.equalsIgnoreCase(PartitionerInputCommands.GREEDY)) {
      h = new Greedy(settings.epsilon);
    } else {
      h = null;
    }

    return h;
  }
}
