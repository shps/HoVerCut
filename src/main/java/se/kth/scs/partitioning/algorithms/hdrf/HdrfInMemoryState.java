package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import se.kth.scs.partitioning.ConcurrentPartition;
import se.kth.scs.partitioning.ConcurrentVertex;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class HdrfInMemoryState implements PartitionState {

  final ConcurrentHashMap<Integer, ConcurrentVertex> vertices = new ConcurrentHashMap<>(); // Holds partial degree of each vertex.
  ConcurrentHashMap<Short, ConcurrentPartition> partitions;
  private final short k;

  public HdrfInMemoryState(short k) {
    this.k = k;
    partitions = new ConcurrentHashMap();
    for (short i = 0; i < k; i++) {
      partitions.put(i, new ConcurrentPartition(i));
    }
  }

  @Override
  public short getNumberOfPartitions() {
    return k;
  }

  @Override
  public void applyState() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void releaseResources() {
    vertices.clear();
    partitions.clear();
  }

  @Override
  public Vertex getVertex(int vid) {
    //TODO: a clone should be sent in multi-threaded version.
    ConcurrentVertex v = vertices.get(vid);
    if (v != null) {
      return v.clone();
    } else {
      return null;
    }
  }

  /**
   * Order of number of vertices.
   *
   * @return
   */
  @Override
  public Map<Integer, Vertex> getAllVertices() {
    Map<Integer, Vertex> copy = new HashMap<>();
    for (ConcurrentVertex v : vertices.values()) {
      copy.put(v.getId(), v.clone());
    }

    return copy;
  }

  @Override
  public Map<Integer, Vertex> getVertices(Set<Integer> vids) {
    Map<Integer, Vertex> someVertices = new HashMap<>();
    for (int vid : vids) {
      someVertices.put(vid, getVertex(vid));
    }

    return someVertices;
  }

  @Override
  public void putVertex(Vertex v) {
    ConcurrentVertex newVertex = new ConcurrentVertex(v.getId());
    newVertex.accumulate(v);
    ConcurrentVertex previous = vertices.putIfAbsent(v.getId(), newVertex);
    // Double check if the entry does not exist.
    if (previous != null) {
      previous.accumulate(v);
    }
  }

  @Override
  public void putVertices(Collection<Vertex> vs) {
    // TODO: update should be accumulated in the multi-threaded version.
    for (Vertex v : vs) {
      putVertex(v);
    }
  }

  @Override
  public Partition getPartition(short pid) {
    ConcurrentPartition p = partitions.get(pid);
    if (p != null) {
      return p.clone();
    } else {
      return null;
    }
  }

  @Override
  public List<Partition> getPartions(short[] pids) {
    List<Partition> somePartitions = new ArrayList<>(pids.length);
    for (short pid : pids) {
      somePartitions.add(getPartition(pid));
    }

    return somePartitions;
  }

  @Override
  public List<Partition> getAllPartitions() {
    List<Partition> copy = new ArrayList();
    for (ConcurrentPartition p : partitions.values()) {
      copy.add(p.clone());
    }

    return copy;
  }

  @Override
  public void putPartition(Partition p) {
    ConcurrentPartition newPartition = new ConcurrentPartition(p.getId());
    newPartition.accumulate(p);
    ConcurrentPartition previous = partitions.putIfAbsent(p.getId(), newPartition);
    // Double check if the entry does not exist.
    if (previous != null) {
      previous.accumulate(p);
    }
  }

  @Override
  public void putPartitions(List<Partition> ps) {
    for (Partition p : ps) {
      putPartition(p);
    }
  }

  @Override
  public void releaseTaskResources() {
    // No data is stored per thread.
  }

}
