package se.kth.scs.partitioning.algorithms.hdrf;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class HdrfMySqlQueries {

    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    public static final String DEFAULT_DB_URL = "jdbc:mysql://localhost/hdrf2";
    public static final String DEFAULT_USER = "root";
    public static final String DEFAULT_PASS = "";
    // Database structure

    public static final String DB = "hdrf";
    public static final String VERTEX_PARTITION = "vertex_partition";
    public static final String PARTITIONS = "partitions";
    public static final String VERTICES = "vertices";
    public static final String VID = "vid";
    public static final String PID = "pid";
    public static final String EDGE_SIZE = "edge_size";
    public static final String VERTEX_SIZE = "vertex_size";
    public static final String PARTIAL_DEGREE = "partial_degree";
    // Queries
    private static final String INSERT_VERTEX_PARTITION = String.format("insert ignore into %s (%s, %s) values (?,?)", VERTEX_PARTITION, VID, PID);
    // This query just counts the number of edges. It keeps the number of partitions zero. 
    // This is because, we can count the number of partitions by counting on vertex_partition table.
    private static final String UPDATE_PARTITION = String.format("insert into %s (%s, %s, %s) values (?, 1, 0) on duplicate key update %s=%s+1",
            PARTITIONS, PID, EDGE_SIZE, VERTEX_SIZE, EDGE_SIZE, EDGE_SIZE);
    private static final String UPDATE_VERTEX = String.format("insert into %s (%s, %s) values (?, 1) on duplicate key update %s=%s+1",
            VERTICES, VID, PARTIAL_DEGREE, PARTIAL_DEGREE, PARTIAL_DEGREE);
    private static final String QUERY_EDGE_SIZE = String.format("select %s from %s where %s=?", EDGE_SIZE, PARTITIONS, PID);
    private static final String QUERY_VERTEX_SIZE = String.format("select count(*) from %s where %s=?", VERTEX_PARTITION, PID);
    private static final String CONTAINS_VERTEX = String.format("select count(*) from %s where %s=? and %s=?", VERTEX_PARTITION, VID, PID);
    private static final String QUERY_VERTICES = String.format("select %s from %s where %s=?", VID, VERTEX_PARTITION, PID);
    private static final String QUERY_PARTITIONS = String.format("select %s from %s where %s=?", PID, VERTEX_PARTITION, VID);
    private static final String QUERY_VERTEX_DEGREE = String.format("select %s from %s where %s=?", PARTIAL_DEGREE, VERTICES, VID);
    private static final String MAX_PARTITION_VERTEX_SIZE = String.format("select MAX(%s) from %s", VERTEX_SIZE, PARTITIONS);
    private static final String MAX_PARTITION_EDGE_SIZE = String.format("select MAX(%s) from %s", EDGE_SIZE, PARTITIONS);

    public static Vertex getVertex(long vid, Connection con) throws SQLException {
        String query = String.format("select * from %s where vid=%d", VERTICES, vid);
        Statement s = con.createStatement();
        ResultSet r = s.executeQuery(query);
        Vertex v = null;
        if (r.next()) {
            int degree = r.getInt(PARTIAL_DEGREE);
            Set<Integer> partitions = deserialize(r.getString(PARTITIONS));
            v = new Vertex(vid, partitions);
            v.setpDegree(degree);
        }

        return v;
    }

    public static Partition getPartition(int pid, Connection con) throws SQLException {
        String query = String.format("select * from %s where pid=%d", PARTITIONS, pid);
        Statement s = con.createStatement();
        ResultSet r = s.executeQuery(query);
        Partition p = null;
        if (r.next()) {
            int vSize = r.getInt(VERTEX_SIZE);
            int eSize = r.getInt(EDGE_SIZE);
            p = new Partition(pid);
            p.setVSize(vSize);
            p.setESize(eSize);
        }

        return p;
    }

    public static Map<Long, Vertex> getVertices(Set<Long> vids, Connection con) throws SQLException {
        StringBuilder query = new StringBuilder(String.format("select * from %s where ", VERTICES));
        int i = 0;
        for (long vid : vids) {
            query.append(String.format(" vid=%d ", vid));
            if (i + 1 < vids.size()) {
                query.append("or");
            }
            i++;
        }
        Statement s = con.createStatement();
        ResultSet r = s.executeQuery(query.toString());
        Map<Long, Vertex> vertices = new HashMap();
        while (r.next()) {
            long vid = r.getLong(VID);
            int degree = r.getInt(PARTIAL_DEGREE);
            Set<Integer> partitions = deserialize(r.getString(PARTITIONS));
            Vertex v = new Vertex(vid, partitions);
            v.setpDegree(degree);
            vertices.put(vid, v);
        }

        return vertices;
    }

    public static Map<Long, Vertex> getAllVertices(Connection con) throws SQLException {
        String query = String.format("select * from %s", VERTICES);

        Statement s = con.createStatement();
        ResultSet r = s.executeQuery(query);
        Map<Long, Vertex> vertices = new HashMap();
        while (r.next()) {
            long vid = r.getLong(VID);
            int degree = r.getInt(PARTIAL_DEGREE);
            Set<Integer> partitions = deserialize(r.getString(PARTITIONS));
            Vertex v = new Vertex(vid, partitions);
            v.setpDegree(degree);
            vertices.put(vid, v);
        }

        return vertices;
    }

    public static List<Partition> getPartitions(int[] pids, Connection con) throws SQLException {
        StringBuilder query = new StringBuilder(String.format("select * from %s where ", PARTITIONS));
        for (int i = 0; i < pids.length; i++) {
            query.append(String.format(" pid=%d ", pids[i]));
            if (i + 1 < pids.length) {
                query.append("or");
            }
        }
        Statement s = con.createStatement();
        ResultSet r = s.executeQuery(query.toString());
        LinkedList<Partition> partitions = new LinkedList<>();
        while (r.next()) {
            int pid = r.getInt(PID);
            int vSize = r.getInt(VERTEX_SIZE);
            int eSize = r.getInt(EDGE_SIZE);
            Partition p = new Partition(pid);
            p.setESize(eSize);
            p.setVSize(vSize);
            partitions.add(p);
        }

        return partitions;
    }

    public static List<Partition> getAllPartitions(Connection con) throws SQLException {
        String query = String.format("select * from %s", PARTITIONS);
        Statement s = con.createStatement();
        ResultSet r = s.executeQuery(query);
        ArrayList<Partition> partitions = new ArrayList<>();
        while (r.next()) {
            int pid = r.getInt(PID);
            int vSize = r.getInt(VERTEX_SIZE);
            int eSize = r.getInt(EDGE_SIZE);
            Partition p = new Partition(pid);
            p.setESize(eSize);
            p.setVSize(vSize);
            partitions.add(p);
        }

        return partitions;
    }

    public static int putVertex(Vertex v, Connection con) throws SQLException {
        String query = createPutVertexQuery(v);
        System.out.println(query);
        Statement s = con.createStatement();
        int r = s.executeUpdate(query);
        s.closeOnCompletion();
        return r;
    }

    public static int putPartition(Partition p, Connection con) throws SQLException {
        String query = createPutPartitionQuery(p);
        Statement s = con.createStatement();
        int r = s.executeUpdate(query);
        s.closeOnCompletion();
        return r;
    }

    private static String createPutVertexQuery(Vertex v) {
        long vid = v.getId();
        int pDegree = v.getpDegree();
        String partitions = serialize(v.getPartitions());
        StringBuilder query
                = new StringBuilder(String.format("insert into %s (vid, partial_degree, partitions) values (%d, %d, \"%s\") "
                                + "on duplicate key update partial_degree=partial_degree+%d",
                                VERTICES, vid, pDegree, partitions, v.getDegreeDelta()));
        if (!v.getPartitionsDelta().isEmpty()) {
            String partitionsDelta = serialize(v.getPartitionsDelta());
            query.append(String.format(", partitions=concat(partitions,\",\",\"%s\")", partitionsDelta));
        }

        return query.toString();
    }

    public static int[] putVertices(Collection<Vertex> vertices, Connection con) throws SQLException {
        Statement s = con.createStatement();
        for (Vertex v : vertices) {
            String query = createPutVertexQuery(v);
            s.addBatch(query);
        }
        int[] r = s.executeBatch();
        s.closeOnCompletion();

        return r;
    }

    public static int[] putPartitions(List<Partition> partitions, Connection con) throws SQLException {
        Statement s = con.createStatement();
        for (Partition p : partitions) {
            String query = createPutPartitionQuery(p);
            s.addBatch(query);
        }
        int[] r = s.executeBatch();
        s.closeOnCompletion();

        return r;

    }

    private static Set<Integer> deserialize(String partitions) {
        Set<Integer> pSet = new HashSet<>();
        if (!partitions.isEmpty()) {
            String[] items = partitions.split(",");
            for (String item : items) {
                pSet.add(Integer.valueOf(item));
            }
        }

        return pSet;
    }

    private static String serialize(Set<Integer> partitions) {
        StringBuilder sb = new StringBuilder("");
        int i = 0;
        for (int p : partitions) {
            sb.append(p);
            if (i + 1 < partitions.size()) {
                sb.append(",");
            }
            i++;
        }

        return sb.toString();
    }

    private static String createPutPartitionQuery(Partition p) {
        int pid = p.getId();
        int vSize = p.getVSize();
        int eSize = p.getESize();
        int vDelta = p.getVSizeDelta();
        int eDelta = p.getESizeDelta();
        String query
                = String.format("insert into %s (pid, vertex_size, edge_size) values (%d, %d, %d) "
                        + "on duplicate key update vertex_size=vertex_size+%d, edge_size=edge_size+%d",
                        PARTITIONS, pid, vSize, eSize, vDelta, eDelta);
        return query;
    }

    public static int[] clearAllTables(Connection con) throws SQLException {
        String truncVertices = String.format("truncate table %s", VERTICES);
        String truncPartitions = String.format("truncate table %s", PARTITIONS);
        Statement s = con.createStatement();
        s.addBatch(truncVertices);
        s.addBatch(truncPartitions);
        return s.executeBatch();
    }
}
