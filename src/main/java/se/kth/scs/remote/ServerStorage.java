package se.kth.scs.remote;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
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

  final ConcurrentHashMap<Long, ConcurrentVertex> vertices = new ConcurrentHashMap<>(); // Holds partial degree of each vertex.
  private final ConcurrentHashMap<Integer, ConcurrentPartition> partitions;
  private final int k;

  public ServerStorage(int k) {
    this.k = k;
    partitions = new ConcurrentHashMap();
    initPartitions(partitions, this.k);
  }

  private void initPartitions(ConcurrentHashMap<Integer, ConcurrentPartition> partitions, int k) {
    partitions.clear();
    for (int i = 0; i < k; i++) {
      partitions.put(i, new ConcurrentPartition(i));
    }
  }

  public int getNumberOfPartitions() {
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
    long[] vIds = new long[size];
    int[] degrees = new int[size];
    byte[] ps = new byte[size];
    int i = 0;
    for (ConcurrentVertex v : vertices.values()) {
      vIds[i] = v.getId();
      degrees[i] = v.getpDegree();
      ps[i] = v.getPartitions();
      i++;
    }

    return new VerticesReadResponse(vIds, degrees, ps);
  }

  public Vertex getVertex(long vid) {
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
    for (long vid : request.getVertices()) {
      Vertex v = getVertex(vid);
      if (v != null) {
        vs.add(v);
      }
    }
    long[] vIds = new long[vs.size()];
    int[] degrees = new int[vs.size()];
    byte[] ps = new byte[vs.size()];
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
      ConcurrentVertex newShared = new ConcurrentVertex(v.getId(), (byte) 0);
      newShared.accumulate(v);
      newShared = vertices.putIfAbsent(v.getId(), newShared);
      // Double check if the entry does not exist.
      if (newShared != null) {
        newShared.accumulate(v);
      }
    }
  }

  public void putVertices(VerticesWriteRequest request) {
    long[] vids = request.getVertices();
    int[] degrees = request.getDegreeDeltas();
    byte[] ps = request.getPartitionsDeltas();
    for (int i = 0; i < vids.length; i++) {
      Vertex v = new Vertex(vids[i]);
      v.setDegreeDelta(degrees[i]);
      v.setPartitionsDelta(ps[i]);
      putVertex(v);
    }
  }

  public Partition getPartition(int pid) {
    ConcurrentPartition p = partitions.get(pid);
    if (p != null) {
      return p.clone();
    } else {
      return null;
    }
  }

  public PartitionsResponse getPartitions(PartitionsRequest request) {
    int[] eSizes = new int[k];
    int[] vSizes = new int[k];
    for (int i = 0; i < k; i++) {
      Partition p = getPartition(i);
      if (p == null) {
        eSizes[i] = 0;
        vSizes[i] = 0;
      } else {
        eSizes[i] = p.getESize();
        vSizes[i] = p.getVSize();
      }
    }

    return new PartitionsResponse(eSizes, vSizes);
  }

  public void putPartition(Partition p) {
    ConcurrentPartition shared = partitions.get(p.getId());
    if (shared != null) {
      shared.accumulate(p);
    } else {
      ConcurrentPartition newShared = new ConcurrentPartition(p.getId());
      newShared.accumulate(p);
      newShared = partitions.putIfAbsent(p.getId(), newShared);
      // Double check if the entry does not exist.
      if (newShared != null) {
        newShared.accumulate(p);
      }
    }
  }

  public void putPartitions(PartitionsWriteRequest request) {
    int[] eSizes = request.geteDeltas();
    int[] vSizes = request.getvDeltas();
    for (int i = 0; i < eSizes.length; i++) {
      Partition p = new Partition(i);
      p.seteSizeDelta(eSizes[i]);
      p.setvSizeDelta(vSizes[i]);
      putPartition(p);
    }
  }

}
