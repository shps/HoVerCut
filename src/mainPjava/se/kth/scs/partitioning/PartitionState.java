package se.kth.scs.partitioning;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The interface for maintaining state of the multi-loader HDRF.
 *
 * @author Hooman
 */
public interface PartitionState {

  /**
   * Returns number of partitions.
   *
   * @return
   */
  public short getNumberOfPartitions();

  /**
   * This method can be implemented to commit the state in a transactional
   * storage.
   *
   */
  public void applyState();

  /**
   * This method can be implemented to release all the resources created by the
   * loaders. E.g., closing a session to the database.
   *
   */
  public void releaseResources();

  /**
   * This method can be used in the scope of loaders, to release the resources
   * taken in the state storage.
   *
   */
  public void releaseTaskResources();

  /**
   * Returns state of a vertex using its unique ID.
   *
   * @param vid
   * @return
   */
  public Vertex getVertex(int vid);

  /**
   * Returns the state of all vertices for at least a given number of vertices.
   *
   *
   * @param expectedSize
   * @return
   */
  public Map<Integer, Vertex> getAllVertices(int expectedSize);

  /**
   * Given a set of ID, returns all the state of all the available vertices in
   * the state storage.
   *
   * @param vids
   * @return
   */
  public Map<Integer, Vertex> getVertices(Set<Integer> vids);

  /**
   *
   *
   * @param v
   */
  public void putVertex(Vertex v);

  /**
   *
   *
   * @param vs
   */
  public void putVertices(Collection<Vertex> vs);

  /**
   *
   *
   * @param pid
   * @return
   */
  public Partition getPartition(short pid);

  public List<Partition> getPartions(short[] pids);

  public List<Partition> getAllPartitions();

  public void putPartition(Partition p);

  public void putPartitions(List<Partition> p);
}
