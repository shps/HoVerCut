package se.kth.scs.partitioning;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Hooman
 */
public interface PartitionState {

  public short getNumberOfPartitions();

  public void applyState();

  public void releaseResources();

  public void releaseTaskResources();

  public Vertex getVertex(int vid);

  public Map<Integer, Vertex> getAllVertices(int expectedSize);

  public Map<Integer, Vertex> getVertices(Set<Integer> vids);

  public void putVertex(Vertex v);

  public void putVertices(Collection<Vertex> vs);

  public Partition getPartition(short pid);

  public List<Partition> getPartions(short[] pids);

  public List<Partition> getAllPartitions();

  public void putPartition(Partition p);

  public void putPartitions(List<Partition> p);
}
