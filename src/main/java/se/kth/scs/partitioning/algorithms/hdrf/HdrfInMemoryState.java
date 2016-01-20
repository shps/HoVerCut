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

  private final int INIT_CAPACITY = 12000000;
  private final float LOAD_FACTOR = 0.75f;
  private final ConcurrentHashMap<Integer, ConcurrentVertex> vertices; // Holds partial degree of each vertex.
  private final ConcurrentHashMap<Short, ConcurrentPartition> partitions;
  private final short k;

  public HdrfInMemoryState(final short k, final int nThreads) {
    this.k = k;
    vertices = new ConcurrentHashMap<>(INIT_CAPACITY, LOAD_FACTOR, nThreads);
    partitions = new ConcurrentHashMap<>(k, 1, nThreads);
    initPartitions();
  }

  private void initPartitions() {
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
  public void releaseResources(final boolean clearAll) {
    if (clearAll) {
      vertices.clear();
    } else {
      for (ConcurrentVertex v : vertices.values()) {
        v.resetPartition();
      }
    }
    initPartitions();
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

  @Override
  public Map<Integer, Vertex> getVertices(final Set<Integer> vids) {
    Map<Integer, Vertex> someVertices = new HashMap<>();
    for (int vid : vids) {
      someVertices.put(vid, getVertex(vid));
    }

    return someVertices;
  }

  @Override
  public void putVertex(final Vertex v) {
    ConcurrentVertex shared = vertices.get(v.getId());
    if (shared != null) {
      shared.accumulate(v);
    } else {
      final ConcurrentVertex newVertex = new ConcurrentVertex(v.getId(), 0);
      newVertex.accumulate(v);
      shared = vertices.putIfAbsent(v.getId(), newVertex);
      // Double check if the entry does not exist.
      if (shared != null) {
        shared.accumulate(v);
      }
    }
  }

  @Override
  public void putVertices(final Collection<Vertex> vs) {
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
  public void putPartition(final Partition p) {
    final ConcurrentPartition shared = partitions.get(p.getId());
//    if (shared != null) {
    shared.accumulate(p); //It shoudl always exist.
//    } else {
//      shared = new ConcurrentPartition(p.getId());
//      shared.accumulate(p);
//      shared = partitions.putIfAbsent(p.getId(), shared);
//      // Double check if the entry does not exist.
//      if (shared != null) {
//        shared.accumulate(p);
//      }
//    }
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

  @Override
  public Map<Integer, Vertex> getAllVertices(int expectedSize) {
    waitForAllUpdates(expectedSize);
    Map<Integer, Vertex> copy = new HashMap<>();
    for (ConcurrentVertex v : vertices.values()) {
      copy.put(v.getId(), v.clone());
    }

    return copy;
  }

  @Override
  public void waitForAllUpdates(int expectedSize) {
    int count = 1;
    while (vertices.size() < expectedSize) {
      try {
        Thread.sleep(1);
        System.out.println(String.format("Try number %d failed to collect expected number of vertices.", count));
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
      count++;
    }
  }

}
