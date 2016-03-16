package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.policy.Hdrf;
import se.kth.scs.partitioning.policy.Greedy;
import se.kth.scs.partitioning.policy.PartitionSelectionPolicy;
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
        "-f ./data/datasets/Cit-HepTh.txt -w 10 -p 4 -t 2  -s remote -db localhost:4444"));
      System.exit(1);
    }

    runPartitioning(commands);
    output.writeToFile(commands.output, commands.append);
  }

  private static void runPartitioning(PartitionerInputCommands commands) throws IOException, Exception {
    PartitionerSettings settings = new PartitionerSettings();
    settings.setSettings(commands);

    System.out.println(String.format("Reading file %s", settings.file));
    long start = System.currentTimeMillis();
    long seed = start;
    EdgeFileReader reader = new EdgeFileReader(settings.delimiter);
    LinkedHashSet<Edge>[] splits = reader.readSplitFile(settings.file, 1, settings.shuffle, seed);
    System.out.println(String.format("Finished reading in %d seconds.", (System.currentTimeMillis() - start) / 1000));

    System.out.println(String.format("Re-splitting file %s", settings.file));
    start = System.currentTimeMillis();
    splits = EdgeFileReader.resplit(splits, settings.tasks, reader.getnEdges());
    int nEdges = reader.getnEdges();
    int nVertices = reader.getnVertices();
    System.out.println(String.format("Finished resplitting in %d seconds.", (System.currentTimeMillis() - start) / 1000));
    if (settings.window <= 0) {
      settings.window = nEdges / settings.tasks;
    }
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
      settings.window,
      settings.tasks,
      seed,
      duration);
    output.addResult(result);
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
    boolean exactDegree = settings.exactDegree;
    PartitionSelectionPolicy heuristic = buildHeuristic(settings);
    state = prepareState(settings, state, exactDegree);
    HovercutPartitioner.partitionWithWindow(
      state,
      splits,
      heuristic,
      settings.window,
      settings.frequency,
      exactDegree);
    if (exactDegree == false) {
      state.waitForAllUpdates(nVertices);
      state.releaseTaskResources();
    }
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

  private static PartitionSelectionPolicy buildHeuristic(PartitionerSettings settings) {
    PartitionSelectionPolicy h = null;
    if (settings.algorithm.equalsIgnoreCase(PartitionerInputCommands.HDRF)) {
      h = new Hdrf(settings.lambda, settings.epsilon);
    } else if (settings.algorithm.equalsIgnoreCase(PartitionerInputCommands.GREEDY)) {
      h = new Greedy(settings.epsilon);
    }

    return h;
  }
}
