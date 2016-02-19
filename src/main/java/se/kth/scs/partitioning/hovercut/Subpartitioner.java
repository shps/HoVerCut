package se.kth.scs.partitioning.hovercut;

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
import se.kth.scs.partitioning.heuristics.Heuristic;

/**
 * This is an implementation of a partitioning loader.
 *
 * @author Ganymedian
 */
public class Subpartitioner implements Runnable {

  private LinkedHashSet<Edge> edges;
  private final Heuristic heuristic;
  private final int windowSize;
  private final PartitionState state;
  private final int minDelay;
  private final int maxDelay;
  private final int pUpdateFrequency;
  private final boolean srcGrouping;
  private final boolean exactDegree;

  private final LinkedList<Edge>[] assignments;

  public Subpartitioner(
    PartitionState state,
    LinkedHashSet<Edge> edges,
    Heuristic heuristic,
    int windowSize,
    int minDelay,
    int maxDelay,
    int pUpdateFrequency,
    boolean srcGrouping,
    boolean exactDegree) {
    this.edges = edges;
    this.heuristic = heuristic;
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
        allocateNextWindow(edgeWindow, vertices, state, partitionsWindow);
        edgeWindow.clear();
        vertices.clear();
      }
      counter++;
    }

    if (!edgeWindow.isEmpty()) {
      allocateNextWindow(edgeWindow, vertices, state, partitionsWindow);
      edgeWindow.clear();
      vertices.clear();
    }

    return state;
  }

  private void allocateNextWindow(
    final List<Edge> edgeWindow,
    final Set<Integer> vIds,
    final PartitionState state,
    final int partitionWindow) {
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
      Partition assignedPartition = heuristic.allocateNextEdge(u, v, partitions);
      u.addPartition(assignedPartition.getId());
      v.addPartition(assignedPartition.getId());
      assignedPartition.incrementESize();
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
