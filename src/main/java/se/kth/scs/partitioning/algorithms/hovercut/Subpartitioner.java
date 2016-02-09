package se.kth.scs.partitioning.algorithms.hovercut;

import java.util.HashMap;
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
 * This is an implementation of a partitioning loader.
 *
 * @author Ganymedian
 */
public class Subpartitioner implements Runnable {

  private LinkedHashSet<Edge> edges;
  private final double lambda;
  private final double epsilon;
  private final int windowSize;
  private final PartitionState state;
  private final int minDelay;
  private final int maxDelay;
  private final int pUpdateFrequency;
  private final boolean srcGrouping;
  private final boolean exactDegree;

  private final LinkedList<Edge>[] assignments;

  public Subpartitioner(PartitionState state, LinkedHashSet<Edge> edges, double lambda, double epsilon, int windowSize) {
    this(state, edges, lambda, epsilon, windowSize, 0, 0, false, false);
  }

  public Subpartitioner(
    PartitionState state,
    LinkedHashSet<Edge> edges,
    double lambda,
    double epsilon,
    int windowSize,
    int minDelay,
    int maxDelay,
    boolean srcGrouping,
    boolean restream) {
    this(state, edges, lambda, epsilon, windowSize, minDelay, maxDelay, (short) 0, srcGrouping, restream);

  }

  public Subpartitioner(
    PartitionState state,
    LinkedHashSet<Edge> edges,
    double lambda,
    double epsilon,
    int windowSize,
    int minDelay,
    int maxDelay,
    int pUpdateFrequency,
    boolean srcGrouping,
    boolean exactDegree) {
    this.edges = edges;
    this.lambda = lambda;
    this.epsilon = epsilon;
    this.windowSize = windowSize;
    this.state = state;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
    this.pUpdateFrequency = pUpdateFrequency;
    this.srcGrouping = srcGrouping;
    this.exactDegree = exactDegree;
    this.assignments = new LinkedList[state.getNumberOfPartitions()];
    for (int i = 0; i < state.getNumberOfPartitions(); i++) {
      this.assignments[i] = new LinkedList<>();
    }
  }

  /**
   * 
   *
   * @return
   */
  public PartitionState partitionWithWindow() {
    if (srcGrouping) {
      this.edges = applyEdgeSourceGrouping(this.edges);
    }
    int counter = 1;
    List<Edge> edgeWindow = new LinkedList<>();
    Set<Integer> vertices = new HashSet();
    int partitionsWindow = windowSize / pUpdateFrequency;
    for (Edge e : edges) {
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

    return state;
  }

  private void allocateNextWindow(
    List<Edge> edgeWindow,
    Set<Integer> vIds,
    PartitionState state,
    double lambda,
    double epsilon,
    int partitionWindow) {
    Map<Integer, Vertex> vertices = state.getVertices(vIds);
    List<Partition> partitions = state.getAllPartitions();
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
      if (!exactDegree) {
        u.incrementDegree();
        v.incrementDegree();
      }
      Partition assignedPartition = allocateNextEdge(u, v, partitions, lambda, epsilon);
      this.assignments[assignedPartition.getId()].add(e);
      if (counter % partitionWindow == 0 && counter < size) {
        state.putPartitions(partitions);
        partitions = state.getAllPartitions();
      }
      counter++;
    }

    //TODO: Inconsistency if update partitions and vertices separately.
    state.putPartitions(partitions);
    state.putVertices(vertices.values());
  }

  private Partition allocateNextEdge(Vertex v1, Vertex v2, List<Partition> partitions, double lambda, double epsilon) {

    int deltaV1 = v1.getpDegree();
    int deltaV2 = v2.getpDegree();
    if (deltaV1 == 0 || deltaV2 == 0)
    {
      System.err.println("Warning, vertex degree is zero!");
    }
    double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
    double thetaV2 = 1 - thetaV1;

    // Compute C score for each partition.
    double maxScore = Long.MIN_VALUE;
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
      double sRep = computeReplicationScore(p, v1, v2, thetaV1, thetaV2);
      double sBal = computeBalanceScore(p, maxSize, minSize, lambda, epsilon);

      double score = sRep + sBal;
      if (score > maxScore) {
        maxScore = score;
        maxPartition = p;
      }
    }

    if (maxPartition == null)
    {
      System.out.println();
    }
    //MaxPartition should not be null
    v1.addPartition(maxPartition.getId());
    v2.addPartition(maxPartition.getId());
    maxPartition.incrementESize();
    return maxPartition;
  }

  public static double computeReplicationScore(Partition p, Vertex v, Vertex u, double thetaV, double thetaU) {
    return g(p, v, thetaV) + g(p, u, thetaU);
  }

  private static double g(Partition p, Vertex v, double thetaV) {
    if (!v.containsPartition(p.getId())) {
      return 0;
    }

    return 1 + (1 - thetaV);
  }

  private static double computeBalanceScore(Partition p, int maxSize, int minSize, double lambda, double epsilon) {
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

  public LinkedList<Edge>[] getAssignments() {
    return assignments;
  }

  /**
   * Put edges in a different bucket based on its source id.
   *
   * @param edges
   * @return
   */
  private LinkedHashSet<Edge> applyEdgeSourceGrouping(LinkedHashSet<Edge> edges) {
    long id = Thread.currentThread().getId();
    System.out.println(String.format("Task%d started to do source grouping.", id));
    long start = System.currentTimeMillis();
    Map<Integer, LinkedList<Edge>> buckets = new HashMap<>();
    for (Edge e : edges) {
      LinkedList<Edge> list;
      if (buckets.containsKey(e.getSrc())) {
        list = buckets.get(e.getSrc());
      } else {
        list = new LinkedList<>();
        buckets.put(e.getSrc(), list);
      }

      list.add(e);
    }

    LinkedHashSet<Edge> groupedEdges = new LinkedHashSet<>();
    for (LinkedList<Edge> l : buckets.values()) {
      groupedEdges.addAll(l);
    }
    System.out.println(String.format("Task%d finished source grouping in %d seconds.",
      id, (System.currentTimeMillis() - start) / 1000));
    return groupedEdges;
  }
}
