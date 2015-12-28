package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class ExactDegreeTask implements Runnable {

  private final PartitionState state;
  private final LinkedHashSet<Edge> edges;

  public ExactDegreeTask(PartitionState state, LinkedHashSet<Edge> edges) {
    this.state = state;
    this.edges = edges;
  }

  @Override
  public void run() {
    Map<Integer, Vertex> vertices = new HashMap();
    for (Edge e : edges) {
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
    }

    state.putVertices(vertices.values());
    state.releaseTaskResources();
  }

}
