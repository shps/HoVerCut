
package se.kth.scs.partitioning.heuristics;

import java.util.List;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public interface Heuristic {
  
  public Partition allocateNextEdge(Vertex v1, Vertex v2, List<Partition> partitions);
}
