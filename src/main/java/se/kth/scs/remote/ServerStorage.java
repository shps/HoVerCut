package se.kth.scs.remote;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import se.kth.scs.partitioning.ConcurrentPartition;
import se.kth.scs.partitioning.ConcurrentVertex;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class ServerStorage {

  private final static short NUM_TRIES = 5;
  private final ConcurrentHashMap<Integer, ConcurrentVertex> vertices = new ConcurrentHashMap<>(); // Holds partial degree of each vertex.
  private final ConcurrentHashMap<Short, ConcurrentPartition> partitions;
  private final short k;

  public ServerStorage(short k) {
    this.k = k;
    partitions = new ConcurrentHashMap();
    initPartitions(partitions, this.k);
  }

  private void initPartitions(ConcurrentHashMap<Short, ConcurrentPartition> partitions, short k) {
    partitions.clear();
    for (short i = 0; i < k; i++) {
      partitions.put(i, new ConcurrentPartition(i));
    }
  }

  public short getNumberOfPartitions() {
    return k;
  }

  public void releaseResources() {
    vertices.clear();
    initPartitions(partitions, k);
  }

  /**
   * Order of number of vertices. It serializes for efficiency.
   *
   * @param expectedSize
   * @return
   */
  public int[] getAllVertices(int expectedSize) {

    // To work-around the concurrenthashmap's weak consistency,
    // that affects inconsistent results between values().size() and the iterator over the valus.
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
    int[] array = new int[vertices.size() * 3];
    int i = 0;
    for (ConcurrentVertex v : vertices.values()) {
      array[i] = v.getId();
      array[i + 1] = v.getpDegree();
      array[i + 2] = v.getPartitions();
      i = i + 3;
    }

    return array;
  }

  public Vertex getVertex(int vid) {
    //TODO: a clone should be sent in multi-threaded version.
    ConcurrentVertex v = vertices.get(vid);
    if (v != null) {
      return v.clone();
    } else {
      return null;
    }
  }

  public LinkedList<Vertex> getVertices(int[] vids) {
    LinkedList<Vertex> vs = new LinkedList<>();
    for (int vid : vids) {
      Vertex v = getVertex(vid);
      if (v != null) {
        vs.add(v);
      }
    }
    return vs;
  }

  public void putVertex(Vertex v) {
    ConcurrentVertex shared = vertices.get(v.getId());
    if (shared != null) {
      shared.accumulate(v);
    } else {
      shared = new ConcurrentVertex(v.getId(), 0);
      shared.accumulate(v);
      shared = vertices.putIfAbsent(v.getId(), shared);
      // Double check if the entry does not exist.
      if (shared != null) {
        shared.accumulate(v);
      }
    }
  }

  public void putVertices(int[] vertices) {
    for (int i = 0; i < vertices.length; i = i + 3) {
      Vertex v = new Vertex(vertices[i]);
      v.setDegreeDelta(vertices[i + 1]);
      v.setPartitionsDelta(vertices[i + 2]);
      putVertex(v);
    }
  }

  public Partition getPartition(short pid) {
    ConcurrentPartition p = partitions.get(pid);
    if (p != null) {
      return p.clone();
    } else {
      return null;
    }
  }

  public int[] getPartitions() {
    int[] eSizes = new int[k];
    for (short i = 0; i < k; i++) {
      Partition p = getPartition(i);
      if (p == null) {
        eSizes[i] = 0;
      } else {
        eSizes[i] = p.getESize();
      }
    }

    return eSizes;
  }

  public void putPartition(Partition p) {
    ConcurrentPartition shared = partitions.get(p.getId());
    if (shared != null) {
      shared.accumulate(p);
    } else {
      shared = new ConcurrentPartition(p.getId());
      shared.accumulate(p);
      shared = partitions.putIfAbsent(p.getId(), shared);
      // Double check if the entry does not exist.
      if (shared != null) {
        shared.accumulate(p);
      }
    }
  }

  public void putPartitions(int[] eSizes) {
    for (short i = 0; i < eSizes.length; i++) {
      Partition p = new Partition(i);
      p.seteSizeDelta(eSizes[i]);
      putPartition(p);
    }
  }

}
