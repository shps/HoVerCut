package se.kth.scs.remote;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import se.kth.scs.partitioning.ConcurrentPartition;
import se.kth.scs.partitioning.ConcurrentVertex;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.Vertex;
import se.kth.scs.remote.messages.PartitionsRequest;
import se.kth.scs.remote.messages.PartitionsResponse;
import se.kth.scs.remote.messages.PartitionsWriteRequest;
import se.kth.scs.remote.messages.VerticesReadRequest;
import se.kth.scs.remote.messages.VerticesReadResponse;
import se.kth.scs.remote.messages.VerticesWriteRequest;

/**
 *
 * @author Hooman
 */
public class ServerStorage {

  final ConcurrentHashMap<Integer, ConcurrentVertex> vertices = new ConcurrentHashMap<>(); // Holds partial degree of each vertex.
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
   * Order of number of vertices.
   *
   * @return
   */
  public VerticesReadResponse getAllVertices() {
    int size = vertices.size();
    int[] vIds = new int[size];
    int[] degrees = new int[size];
    int[] ps = new int[size];
    int i = 0;
    for (ConcurrentVertex v : vertices.values()) {
      vIds[i] = v.getId();
      degrees[i] = v.getpDegree();
      ps[i] = v.getPartitions();
      i++;
    }

    return new VerticesReadResponse(vIds, degrees, ps);
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

  public VerticesReadResponse getVertices(VerticesReadRequest request) {
    LinkedList<Vertex> vs = new LinkedList<>();
    for (int vid : request.getVertices()) {
      Vertex v = getVertex(vid);
      if (v != null) {
        vs.add(v);
      }
    }
    int[] vIds = new int[vs.size()];
    int[] degrees = new int[vs.size()];
    int[] ps = new int[vs.size()];
    for (int i = 0; i < vs.size(); i++) {
      Vertex v = vs.get(i);
      vIds[i] = v.getId();
      degrees[i] = v.getpDegree();
      ps[i] = v.getPartitions();
    }
    return new VerticesReadResponse(vIds, degrees, ps);
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

  public void putVertices(VerticesWriteRequest request) {
    int[] vids = request.getVertices();
    int[] degrees = request.getDegreeDeltas();
    int[] ps = request.getPartitionsDeltas();
    for (int i = 0; i < vids.length; i++) {
      Vertex v = new Vertex(vids[i]);
      v.setDegreeDelta(degrees[i]);
      v.setPartitionsDelta(ps[i]);
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

  public PartitionsResponse getPartitions(PartitionsRequest request) {
    int[] eSizes = new int[k];
    for (short i = 0; i < k; i++) {
      Partition p = getPartition(i);
      if (p == null) {
        eSizes[i] = 0;
      } else {
        eSizes[i] = p.getESize();
      }
    }

    return new PartitionsResponse(eSizes);
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

  public void putPartitions(PartitionsWriteRequest request) {
    int[] eSizes = request.geteDeltas();
    for (short i = 0; i < eSizes.length; i++) {
      Partition p = new Partition(i);
      p.seteSizeDelta(eSizes[i]);
      putPartition(p);
    }
  }

}
