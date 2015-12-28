package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;

/**
 * This class is an implementation of a multi-loader HDRF. The first HDRF partitioning method is a single loader, and it
 * is published as: Petroni, Fabio, et al. "HDRF: Stream-Based Partitioning for Power-Law Graphs."
 *
 * @author Hooman
 */
public class HdrfPartitioner {

  public static final double DEFAULT_LAMBDA = 1;
  public static final double DEFAULT_EPSILON = 1;

  /**
   *
   * @param hState
   * @param edges
   * @param lambda
   * @param epsilon
   * @param windowSize
   * @param minDelay
   * @param maxDelay
   * @param pUpdateFrequency
   * @param srcGrouping
   * @param exactDegree
   * @return
   */
  public static LinkedList<Edge>[][] partitionWithWindow(
    PartitionState hState, LinkedHashSet<Edge> edges[],
    double lambda,
    double epsilon,
    int windowSize,
    int minDelay,
    int maxDelay,
    int pUpdateFrequency,
    boolean srcGrouping,
    boolean exactDegree) {
    System.out.println("Starts partitioning...");
    int nTasks = edges.length;
    HdrfPartitionerTask[] tasks = new HdrfPartitionerTask[nTasks];
    Thread[] threads = new Thread[nTasks];
    for (int i = 0; i < nTasks; i++) {
      tasks[i] = new HdrfPartitionerTask(
        hState,
        edges[i],
        lambda,
        epsilon,
        windowSize,
        minDelay,
        maxDelay,
        pUpdateFrequency,
        srcGrouping,
        exactDegree);
      threads[i] = new Thread(tasks[i]);
    }

    long start = System.currentTimeMillis();
    executeThreads(threads);
    System.out.println(String.format("******** Partitioning finished in %d seconds **********", (System.currentTimeMillis() - start) / 1000));

    LinkedList<Edge>[][] outputAssignments = new LinkedList[nTasks][hState.getNumberOfPartitions()];
    for (int i = 0; i < nTasks; i++) {
      outputAssignments[i] = tasks[i].getAssignments();
    }

    return outputAssignments;
  }

  public static void computeExactDegrees(PartitionState hState, LinkedHashSet<Edge> edges[]) {
    int nTasks = edges.length;
    ExactDegreeTask[] tasks = new ExactDegreeTask[nTasks];
    Thread[] threads = new Thread[nTasks];
    for (int i = 0; i < nTasks; i++) {
      tasks[i] = new ExactDegreeTask(hState, edges[i]);
      threads[i] = new Thread(tasks[i]);
    }
    executeThreads(threads);
  }

  private static void executeThreads(Thread[] threads) {
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
  }
}
