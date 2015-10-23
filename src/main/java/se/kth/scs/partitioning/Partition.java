package se.kth.scs.partitioning;

import java.util.HashSet;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * @author Hooman
 */
public class Partition {

    private final HashSet<Long> vertices;
//    private final HashSet<Tuple3<Long, Long, Double>> edges;
    private int edgeSize = 0;
    private final int id;

    public Partition(int id) {
        this.id = id;
        vertices = new HashSet<>();
//        edges = new HashSet<>();
    }

    /**
     * Adds an edge to this partition while cloning end-point vertices in this
     * partition (Vertex-Cut).
     *
     * @param edge
     */
    public void addEdge(Tuple3<Long, Long, Double> edge) {
//        edges.add(edge);
        edgeSize++;
        vertices.add(edge.f0);
        vertices.add(edge.f1);
    }

    /**
     * @return the vertices
     */
    public Long[] getVertices() {
        Long[] copy = new Long[vertices.size()];
        return vertices.toArray(copy);
    }

    public int edgeSize() {
        return edgeSize;
    }

    public int vertexSize() {
        return vertices.size();
    }

    public boolean containsVertex(long vId) {
        return vertices.contains(vId);
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param edgeSize the edgeSize to set
     */
    public void setEdgeSize(int edgeSize) {
        this.edgeSize = edgeSize;
    }
}
