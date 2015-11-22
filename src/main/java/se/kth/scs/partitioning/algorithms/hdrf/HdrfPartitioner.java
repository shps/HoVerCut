package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;

/**
 * This class is an implementation of the HDRF partitioning algorithm. Paper:
 * Petroni, Fabio, et al. "HDRF: Stream-Based Partitioning for Power-Law
 * Graphs."
 *
 * @author Hooman
 */
public class HdrfPartitioner {

  public static final double DEFAULT_LAMBDA = 1;
  public static final double DEFAULT_EPSILON = 1;

  public static LinkedList<Edge>[][] partitionWithWindow(
    PartitionState hState, LinkedHashSet<Edge> edges[],
    double lambda,
    double epsilon,
    int windowSize,
    int minDelay,
    int maxDelay,
    int pUpdateFrequency) {
    System.out.println("Starts partitioning...");
    int nTasks = edges.length;
    HdrfPartitionerTask[] tasks = new HdrfPartitionerTask[nTasks];
    Thread[] threads = new Thread[nTasks];
    for (int i = 0; i < nTasks; i++) {
      tasks[i] = new HdrfPartitionerTask(hState, edges[i], lambda, epsilon, windowSize, minDelay, maxDelay, pUpdateFrequency);
      threads[i] = new Thread(tasks[i]);
    }

    long start = System.currentTimeMillis();
    System.out.println("Start Running Tasks!");
    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException ex) {
        System.out.println(ex.getMessage());
      }
    }

    System.out.println(String.format("******** Partitioning finished in %d seconds **********", (System.currentTimeMillis() - start) / 1000));

    LinkedList<Edge>[][] outputAssignments = new LinkedList[nTasks][hState.getNumberOfPartitions()];
    for (int i = 0; i < nTasks; i++) {
      outputAssignments[i] = tasks[i].getAssignments();
    }

    return outputAssignments;
  }
}
