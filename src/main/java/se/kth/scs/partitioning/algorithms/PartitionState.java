package se.kth.scs.partitioning.algorithms;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * @author Hooman
 */
public interface PartitionState {

    public int getNumberOfPartitions();

    public void applyState();

    public void releaseResources();

    public Vertex getVertex(long vid);

    public Map<Long, Vertex> getAllVertices();

    public Map<Long, Vertex> getVertices(Set<Long> vids);

    public void putVertex(Vertex v);

    public void putVertices(Collection<Vertex> vs);

    public Partition getPartition(int pid);

    public List<Partition> getPartions(int[] pids);

    public List<Partition> getAllPartitions();

    public void putPartition(Partition p);

    public void putPartitions(List<Partition> p);
}
