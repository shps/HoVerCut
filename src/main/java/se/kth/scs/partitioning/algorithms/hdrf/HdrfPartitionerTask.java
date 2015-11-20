package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Ganymedian
 */
public class HdrfPartitionerTask implements Runnable {

  private final LinkedHashSet<Edge> edges;
  private final double lambda;
  private final double epsilon;
  private final int windowSize;
  private final PartitionState state;
//  private final SecureRandom r;
  private final int minDelay;
  private final int maxDelay;
  private final int pUpdateFrequency;

  public HdrfPartitionerTask(PartitionState state, LinkedHashSet<Edge> edges, double lambda, double epsilon, int windowSize) {
    this(state, edges, lambda, epsilon, windowSize, 0, 0);
  }

  public HdrfPartitionerTask(
    PartitionState state,
    LinkedHashSet<Edge> edges,
    double lambda,
    double epsilon,
    int windowSize,
    int minDelay,
    int maxDelay) {
    this(state, edges, lambda, epsilon, windowSize, minDelay, maxDelay, (short) 0);

  }

  public HdrfPartitionerTask(
    PartitionState state,
    LinkedHashSet<Edge> edges,
    double lambda,
    double epsilon,
    int windowSize,
    int minDelay,
    int maxDelay,
    int pUpdateFrequency) {
    this.edges = edges;
    this.lambda = lambda;
    this.epsilon = epsilon;
    this.windowSize = windowSize;
    this.state = state;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
//    r = new SecureRandom();
    this.pUpdateFrequency = pUpdateFrequency;
  }

  /**
   * HDRF (High-Degree Replicated First) partitioning.
   *
   * @return
   */
  public PartitionState partitionWithWindow() {

    int counter = 1;
    List<Edge> edgeWindow = new LinkedList<>();
    Set<Integer> vertices = new HashSet();
    long start = System.currentTimeMillis();
    int partitionsWindow = windowSize / pUpdateFrequency;
    for (Edge e : edges) {
//            System.out.println(String.format("%d Received %d -> %d.", counter, e.f0, e.f1));
      edgeWindow.add(e);
      vertices.add(e.getSrc());
      vertices.add(e.getDst());
      if (counter % windowSize == 0) {
        allocateNextWindow(edgeWindow, vertices, state, lambda, epsilon, partitionsWindow);
        edgeWindow.clear();
        vertices.clear();
      }
      counter++;
    }

    if (!edgeWindow.isEmpty()) {
      allocateNextWindow(edgeWindow, vertices, state, lambda, epsilon, partitionsWindow);
      edgeWindow.clear();
      vertices.clear();
    }

    System.out.println(String.format("******** Task %d finished in %d seconds.", Thread.currentThread().getId(), (System.currentTimeMillis() - start) / 1000));
    return state;
  }

  private void allocateNextWindow(
    List<Edge> edgeWindow,
    Set<Integer> vIds,
    PartitionState state,
    double lambda,
    double epsilon,
    int partitionWindow) {
//        long taskId = Thread.currentThread().getId();
    Map<Integer, Vertex> vertices = state.getVertices(vIds);
//        System.out.println(String.format("Task-%d has read %d vertices.", taskId, vertices.size()));
//        delay(minDelay, maxDelay, r);
    List<Partition> partitions = state.getAllPartitions();
//        System.out.println(String.format("Task-%d has read %d partitions.", taskId, partitions.size()));
//        delay(minDelay, maxDelay, r);
    int counter = 1;
    int size = edgeWindow.size();
    for (Edge e : edgeWindow) {
      Vertex u = vertices.get(e.getSrc());
      Vertex v = vertices.get(e.getDst());
      if (u == null) {
        u = new Vertex(e.getSrc());
        vertices.put(u.getId(), u);
      }
      if (v == null) {
        v = new Vertex(e.getDst());
        vertices.put(v.getId(), v);
      }
      u.incrementDegree();
      v.incrementDegree();
      allocateNextEdge(u, v, partitions, lambda, epsilon);
      if (counter % partitionWindow == 0 && counter < size) {
        state.putPartitions(partitions);
        partitions = state.getAllPartitions();
//        System.out.println(String.format("Task-%d has read %d partitions.", taskId, partitions.size()));
      }
      counter++;
    }

    //TODO: Inconsistency if update partitions and vertices separately.
    state.putPartitions(partitions);
//        System.out.println(String.format("Task-%d updated %d partitions.", taskId, partitions.size()));
//        delay(minDelay, maxDelay, r);
    state.putVertices(vertices.values());
//        System.out.println(String.format("Task-%d updated %d vertices.", taskId, vertices.size()));
//        delay(minDelay, maxDelay, r);
  }

//  private void delay(int min, int max, SecureRandom r) {
//    if (maxDelay > 0) {
//      try {
//        Thread.sleep(min + r.nextInt(max));
//      } catch (InterruptedException ex) {
//        ex.printStackTrace();
//      }
//    }
//  }
  private Partition allocateNextEdge(Vertex v1, Vertex v2, List<Partition> partitions, double lambda, double epsilon) {

    int deltaV1 = v1.getpDegree();
    int deltaV2 = v2.getpDegree();
    double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
    double thetaV2 = 1 - thetaV1;

    // Compute C score for each partition.
    double maxChdrf = Long.MIN_VALUE;
    Partition maxPartition = null;

    int maxSize = Integer.MIN_VALUE;
    int minSize = Integer.MAX_VALUE;

    for (Partition p : partitions) {
      if (p.getESize() > maxSize) {
        maxSize = p.getESize();
      }
      if (p.getESize() < minSize) {
        minSize = p.getESize();
      }
    }

    for (Partition p : partitions) {
      double cRep = computeCReplication(p, v1, v2, thetaV1, thetaV2);
      double cBal = computeCBalance(p, maxSize, minSize, lambda, epsilon);

      double cHdrf = cRep + cBal;
      if (cHdrf > maxChdrf) {
        maxChdrf = cHdrf;
        maxPartition = p;
      }
    }

    //MaxPartition should not be null
    v1.addPartition(maxPartition.getId());
    v2.addPartition(maxPartition.getId());
    maxPartition.incrementESize();

    return maxPartition;
  }

  public static double computeCReplication(Partition p, Vertex v1, Vertex v2, double thetaV1, double thetaV2) {
    return g(p, v1, thetaV1) + g(p, v2, thetaV2);
  }

  private static double g(Partition p, Vertex v1, double thetaV1) {
    if (!v1.containsPartition(p.getId())) {
      return 0;
    }

    return 1 + (1 - thetaV1);
  }

  private static double computeCBalance(Partition p, int maxSize, int minSize, double lambda, double epsilon) {
    int edgeSize = p.getESize();
    return lambda * (maxSize - edgeSize) / (epsilon + maxSize - minSize);
  }

  @Override
  public void run() {
    partitionWithWindow();
    state.releaseTaskResources();
  }

  /**
   * @return the minDelay
   */
  public int getMinDelay() {
    return minDelay;
  }

  /**
   * @return the maxDelay
   */
  public int getMaxDelay() {
    return maxDelay;
  }

}
