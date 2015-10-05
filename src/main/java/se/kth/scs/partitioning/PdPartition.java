package se.kth.scs.partitioning;

import java.util.HashMap;
import org.apache.flink.api.java.tuple.Tuple3;
import utils.HashMapUtil;

/**
 *
 * @author Hooman
 */
public class PdPartition extends Partition {

    private final HashMap<Long, Integer> vDegree = new HashMap<>();

    @Override
    public void addEdge(Tuple3<Long, Long, Double> edge) {
        super.addEdge(edge);
        long v1 = edge.f0;
        long v2 = edge.f1;

        HashMapUtil.increaseValueByOne(vDegree, v1);
        HashMapUtil.increaseValueByOne(vDegree, v2);
    }

    /**
     * Returns the partial degree of a given vertex id that has been added to
     * this partition.
     *
     * @param vId
     * @return
     */
    public int getVertexDegree(long vId) {
        return vDegree.get(vId);
    }
}
