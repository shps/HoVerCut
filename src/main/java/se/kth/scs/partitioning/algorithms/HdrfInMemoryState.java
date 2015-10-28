package se.kth.scs.partitioning.algorithms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Hooman
 */
public class HdrfInMemoryState implements PartitionState {

    final ConcurrentHashMap<Long, ConcurrentVertex> vertices = new ConcurrentHashMap<>(); // Holds partial degree of each vertex.
    ConcurrentHashMap<Integer, ConcurrentPartition> partitions;
    private final int k;

    public HdrfInMemoryState(int k) {
        this.k = k;
        partitions = new ConcurrentHashMap();
        for (int i = 0; i < k; i++) {
            partitions.put(i, new ConcurrentPartition(i));
        }
    }

    @Override
    public int getNumberOfPartitions() {
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
    public Vertex getVertex(long vid) {
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
    public Map<Long, Vertex> getAllVertices() {
        Map<Long, Vertex> copy = new HashMap<>();
        for (ConcurrentVertex v : vertices.values()) {
            copy.put(v.getId(), v.clone());
        }

        return copy;
    }

    @Override
    public Map<Long, Vertex> getVertices(Set<Long> vids) {
        Map<Long, Vertex> someVertices = new HashMap<>();
        for (long vid : vids) {
            someVertices.put(vid, getVertex(vid));
        }

        return someVertices;
    }

    @Override
    public void putVertex(Vertex v) {
        ConcurrentVertex shared = vertices.get(v.getId());
        if (shared != null) {
            shared.accumulate(v);
        } else {
            ConcurrentVertex newShared = new ConcurrentVertex(v.getId(), new HashSet<Integer>());
            newShared.accumulate(v);
            newShared = vertices.putIfAbsent(v.getId(), newShared);
            // Double check if the entry does not exist.
            if (newShared != null) {
                newShared.accumulate(v);
            }
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
    public Partition getPartition(int pid) {
        ConcurrentPartition p = partitions.get(pid);
        if (p != null) {
            return p.clone();
        } else {
            return null;
        }
    }

    @Override
    public List<Partition> getPartions(int[] pids) {
        List<Partition> somePartitions = new ArrayList<>(pids.length);
        for (int pid : pids) {
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

    @Override
    public void putPartitions(List<Partition> ps) {
        for (Partition p : ps) {
            putPartition(p);
        }
    }

}
