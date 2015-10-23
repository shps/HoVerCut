package se.kth.scs.partitioning.algorithms;

import com.mysql.jdbc.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

/**
 *
 * @author Hooman
 */
public class HdrfMySqlQueries {

    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    public static final String DEFAULT_DB_URL = "jdbc:mysql://localhost/hdrf";
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
    // This query just counts the number of edges. It keeps the number of vertices zero. 
    // This is because, we can count the number of vertices by counting on vertex_partition table.
    private static final String UPDATE_PARTITION = String.format("insert into %s (%s, %s, %s) values (?, 1, 0) on duplicate key update %s=%s+1",
            PARTITIONS, PID, EDGE_SIZE, VERTEX_SIZE, EDGE_SIZE, EDGE_SIZE);
    private static final String UPDATE_VERTEX = String.format("insert into %s (%s, %s) values (?, 1) on duplicate key update %s=%s+1",
            VERTICES, VID, PARTIAL_DEGREE, PARTIAL_DEGREE, PARTIAL_DEGREE);
    private static final String QUERY_EDGE_SIZE = String.format("select %s from %s where %s=?", EDGE_SIZE, PARTITIONS, PID);
    private static final String QUERY_VERTEX_SIZE = String.format("select count(*) from %s where %s=?", VERTEX_PARTITION, PID);
    private static final String CONTAINS_VERTEX = String.format("select count(*) from %s where %s=? and %s=?", VERTEX_PARTITION, VID, PID);
    private static final String QUERY_VERTICES = String.format("select %s from %s where %s=?", VID, VERTEX_PARTITION, PID);
    private static final String QUERY_VERTEX_DEGREE = String.format("select %s from %s where %s=?", PARTIAL_DEGREE, VERTICES, VID);
    private static final String MAX_PARTITION_VERTEX_SIZE = String.format("select MAX(%s) from %s", VERTEX_SIZE, PARTITIONS);
    private static final String MAX_PARTITION_EDGE_SIZE = String.format("select MAX(%s) from %s", EDGE_SIZE, PARTITIONS);

    public static void addVertexPartition(long vid, int pid, Connection con) throws SQLException {
        PreparedStatement s1 = con.prepareStatement(INSERT_VERTEX_PARTITION);
        s1.setLong(1, vid);
        s1.setInt(2, pid);
        s1.executeUpdate();
        s1.closeOnCompletion();
    }

    public static void incrementPartitionSize(int pid, Connection con) throws SQLException {
        PreparedStatement s1 = con.prepareStatement(UPDATE_PARTITION);
        s1.setInt(1, pid);
        s1.executeUpdate();
        s1.closeOnCompletion();
    }

    public static void incrementVertexDegree(long vid, Connection con) throws SQLException {
        PreparedStatement s1 = con.prepareStatement(UPDATE_VERTEX);
        s1.setLong(1, vid);
        s1.executeUpdate();
        s1.closeOnCompletion();
    }

    public static int getVertexDegree(long vid, Connection con) throws SQLException {
        PreparedStatement s1 = con.prepareStatement(QUERY_VERTEX_DEGREE);
        s1.setLong(1, vid);
        int d;
        try (ResultSet r = s1.executeQuery()) {
            d = 0;
            if (r.next()) {
                d = r.getInt(PARTIAL_DEGREE);
            }
        }
        s1.closeOnCompletion();

        return d;
    }

    public static Long[] getVerticesForPartition(int pid, Connection con) throws SQLException {
        LinkedList<Long> vertices = new LinkedList<>();

        PreparedStatement s1 = con.prepareStatement(QUERY_VERTICES);
        s1.setInt(1, pid);
        try (ResultSet r = s1.executeQuery()) {
            while (r.next()) {
                vertices.add(r.getLong(VID));
            }
        }
        s1.closeOnCompletion();

        Long[] vs = new Long[vertices.size()];
        return vertices.toArray(vs);
    }

    public static int getEdgeSizeForPartition(int pid, Connection con) throws SQLException {
        int size = 0;
        PreparedStatement s1 = con.prepareStatement(QUERY_EDGE_SIZE);
        s1.setInt(1, pid);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(EDGE_SIZE);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static int getVertexSizeForPartition(int pid, Connection con) throws SQLException {
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(QUERY_VERTEX_SIZE);
        s1.setInt(1, pid);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static boolean partitionContainsVertex(int pid, long vId, Connection con) throws SQLException {
        boolean contains = false;
        // TODO: Optimization, create preparestatemnt once for each connection.
        // Close statements.
        PreparedStatement s1 = con.prepareStatement(CONTAINS_VERTEX);
        s1.setLong(1, vId);
        s1.setInt(2, pid);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                int size = r.getInt(1);
                if (size > 0) {
                    contains = true;
                }
            }
        }
        s1.closeOnCompletion();

        return contains;
    }

    public static int getMaxPartitionEdgeSize(Connection con) throws SQLException {
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(MAX_PARTITION_EDGE_SIZE);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static int getMinPartitionEdgeSize(Connection con) throws SQLException {
//        String MAX_PARTITION_EDGE_SIZE = String.format("select MIN(%s) from %s", EDGE_SIZE, PARTITIONS);
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(MAX_PARTITION_EDGE_SIZE);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static void clearAllTables(Connection con) throws SQLException {
        Statement s1 = con.createStatement();
        s1.executeUpdate("truncate table " + VERTEX_PARTITION);
        s1.executeUpdate("truncate table " + VERTICES);
        s1.executeUpdate("truncate table " + PARTITIONS);
        s1.closeOnCompletion();
    }

    public static int getMaxPartitionVertexSize(Connection con) throws SQLException {
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(MAX_PARTITION_VERTEX_SIZE);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static int getTotalNumberOfReplicas(Connection con) throws SQLException {
        String TOTAL_REPLICAS = String.format("select count(*) from %s", VERTEX_PARTITION);
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(TOTAL_REPLICAS);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static int getTotalNumberOfVertices(Connection con) throws SQLException {
        String TOTAL_REPLICAS = String.format("select count(*) from %s", VERTICES);
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(TOTAL_REPLICAS);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }

    public static int getTotalNumberOfEdges(Connection con) throws SQLException {
        String TOTAL_EDGES = String.format("select sum(%s) from %s", EDGE_SIZE, PARTITIONS);
        int size = 0;

        PreparedStatement s1 = con.prepareStatement(TOTAL_EDGES);
        try (ResultSet r = s1.executeQuery()) {
            if (r.next()) {
                size = r.getInt(1);
            }
        }
        s1.closeOnCompletion();

        return size;
    }
}
