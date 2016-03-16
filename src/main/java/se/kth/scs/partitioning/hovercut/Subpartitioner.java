package se.kth.scs.partitioning.hovercut;

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

  private final LinkedHashSet<Edge> edges;
  private final Heuristic heuristic;
  private final int windowSize;
  private final PartitionState state;
  private final int pUpdateFrequency;
  private final boolean exactDegree;

  private final LinkedList<Edge>[] assignments;

  public Subpartitioner(
    PartitionState state,
    LinkedHashSet<Edge> edges,
    Heuristic heuristic,
    int windowSize,
    int pUpdateFrequency,
    boolean exactDegree) {
    this.edges = edges;
    this.heuristic = heuristic;
    this.windowSize = windowSize;
    this.state = state;
    this.pUpdateFrequency = pUpdateFrequency;
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

    state.putPartitions(partitions);
    state.putVertices(vertices.values());
  }

  @Override
  public void run() {
    partitionWithWindow();
    state.releaseTaskResources();
  }

  public LinkedList<Edge>[] getAssignments() {
    return assignments;
  }

}
