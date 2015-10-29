package se.kth.scs.partitioning.algorithms;

import se.kth.scs.partitioning.Vertex;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *
 * @author Hooman
 */
public class URState implements PartitionState {
    
    // TODO: Implement this class.

    private final Partition[] partitions;
    private final int k;

    public URState(int k) {
        this.k = k;
        partitions = new Partition[k];
        for (int i = 0; i < k; i++) {
            partitions[i] = new Partition(i);
        }
    }

    @Override
    public int getNumberOfPartitions() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void applyState() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void releaseResources() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Vertex getVertex(long vid) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<Long, Vertex> getAllVertices() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<Long, Vertex> getVertices(Set<Long> vids) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putVertex(Vertex v) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putVertices(Collection<Vertex> vs) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Partition getPartition(int pid) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Partition> getPartions(int[] pids) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Partition> getAllPartitions() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putPartition(Partition p) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putPartitions(List<Partition> p) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void releaseTaskResources() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

 

}
