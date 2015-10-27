package se.kth.scs.partitioning.algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Hooman
 */
public class HdrfInMemoryState implements PartitionState {

    final HashMap<Long, Vertex> vertices = new HashMap<>(); // Holds partial degree of each vertex.
    Partition[] partitions;
    private final int k;

    public HdrfInMemoryState(int k) {
        this.k = k;
        partitions = new Partition[k];
        for (int i = 0; i < k; i++) {
            partitions[i] = new Partition(i);
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
        partitions = new Partition[k];
    }

    @Override
    public Vertex getVertex(long vid) {
        //TODO: a clone should be sent in multi-threaded version.
        return vertices.get(vid);
    }

    /**
     * For saving memory, it returns the original copy of the vertices.
     *
     * @return
     */
    @Override
    public Map<Long, Vertex> getAllVertices() {
        return vertices;
    }

    @Override
    public Map<Long, Vertex> getVertices(Set<Long> vids) {
        Map<Long, Vertex> someVertices = new HashMap<>();
        for (long vid : vids) {
            someVertices.put(vid, vertices.get(vid));
        }

        return vertices;
    }

    @Override
    public void putVertex(Vertex v) {
        vertices.put(v.getId(), v);
    }

    @Override
    public void putVertices(Collection<Vertex> vs) {
        // TODO: update should be accumulated in the multi-threaded version.
        for (Vertex v : vs) {
            vertices.put(v.getId(), v);
        }
    }

    @Override
    public Partition getPartition(int pid) {
        return partitions[pid];
    }

    @Override
    public List<Partition> getPartions(int[] pids) {
        List<Partition> somePartitions = new ArrayList<>(pids.length);
        for (int pid : pids) {
            somePartitions.add(partitions[pid]);
        }

        return somePartitions;
    }

    @Override
    public List<Partition> getAllPartitions() {
        return Arrays.asList(partitions);
    }

    @Override
    public void putPartition(Partition p) {
        // TODO: for multi-threaded version, a clone shoudl be sent.
    }

    @Override
    public void putPartitions(List<Partition> p) {
        // TODO: for multi-threaded version, a clone shoudl be sent.
    }

}
