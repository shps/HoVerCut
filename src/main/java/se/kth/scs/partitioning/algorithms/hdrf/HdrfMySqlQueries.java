package se.kth.scs.partitioning.algorithms.hdrf;

import com.mysql.jdbc.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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

  public static Vertex getVertex(int vid, Connection con) throws SQLException {
    String query = String.format("select * from %s where vid=%d", VERTICES, vid);
    Statement s = con.createStatement();
    ResultSet r = s.executeQuery(query);
    Vertex v = null;
    if (r.next()) {
      int degree = r.getInt(PARTIAL_DEGREE);
      int partitions = r.getInt(PARTITIONS);
      v = new Vertex(vid, partitions);
      v.setpDegree(degree);
    }

    return v;
  }

  public static Partition getPartition(short pid, Connection con) throws SQLException {
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

  public static Map<Integer, Vertex> getVertices(Set<Integer> vids, Connection con) throws SQLException {
    StringBuilder query = new StringBuilder(String.format("select * from %s where ", VERTICES));
    int i = 0;
    for (int vid : vids) {
      query.append(String.format(" vid=%d ", vid));
      if (i + 1 < vids.size()) {
        query.append("or");
      }
      i++;
    }
    Statement s = con.createStatement();
    ResultSet r = s.executeQuery(query.toString());
    Map<Integer, Vertex> vertices = new HashMap();
    while (r.next()) {
      int vid = r.getInt(VID);
      int degree = r.getInt(PARTIAL_DEGREE);
      int partitions = r.getInt(PARTITIONS);
      Vertex v = new Vertex(vid, partitions);
      v.setpDegree(degree);
      vertices.put(vid, v);
    }

    return vertices;
  }

  public static Map<Integer, Vertex> getAllVertices(Connection con) throws SQLException {
    String query = String.format("select * from %s", VERTICES);

    Statement s = con.createStatement();
    ResultSet r = s.executeQuery(query);
    Map<Integer, Vertex> vertices = new HashMap();
    while (r.next()) {
      int vid = r.getInt(VID);
      int degree = r.getInt(PARTIAL_DEGREE);
      int partitions = r.getInt(PARTITIONS);
      Vertex v = new Vertex(vid, partitions);
      v.setpDegree(degree);
      vertices.put(vid, v);
    }

    return vertices;
  }

  public static List<Partition> getPartitions(short[] pids, Connection con) throws SQLException {
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
      short pid = r.getShort(PID);
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
      short pid = r.getShort(PID);
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
    int vid = v.getId();
    int pDegree = v.getpDegree();
    String partitions = String.format("%8s", Integer.toBinaryString(v.getPartitions() & 0xFF)).replace(' ', '0');
    String deltaPartitions = String.format("%8s", Integer.toBinaryString(v.getPartitionsDelta() & 0xFF)).replace(' ', '0');
    return String.format("insert into %s (vid, partial_degree, partitions) values (%d, %d, b\'%s\') "
      + "on duplicate key update partial_degree=partial_degree+%d, partitions=partitions | b\'%s\'",
      VERTICES, vid, pDegree, partitions, v.getDegreeDelta(), deltaPartitions);
  }

  public static int[] putVertices(Collection<Vertex> vertices, Connection con) throws SQLException {
    PreparedStatement s = con.prepareStatement(String.format("insert ignore into %s (vid, partial_degree, partitions) values (?, ?, ?) "
      + "on duplicate key update partial_degree=partial_degree+?, partitions=partitions | ?",
      VERTICES));
    for (Vertex v : vertices) {
      s.setInt(1, v.getId());
      s.setInt(2, v.getpDegree());
      s.setInt(3, v.getPartitions());
      s.setInt(4, v.getDegreeDelta());
      s.setInt(5, v.getPartitionsDelta());
      s.addBatch();
    }
    int[] r = s.executeBatch();
    s.closeOnCompletion();

    return r;
  }

  public static int[] putPartitions(List<Partition> partitions, Connection con) throws SQLException {
    PreparedStatement s = con.prepareStatement(String.format("insert into %s (pid, vertex_size, edge_size) values (?, ?, ?) "
      + "on duplicate key update vertex_size=vertex_size+?, edge_size=edge_size+?",
      PARTITIONS));
    for (Partition p : partitions) {
      s.setShort(1, p.getId());
      s.setInt(2, p.getVSize());
      s.setInt(3, p.getESize());
      s.setInt(4, p.getVSizeDelta());
      s.setInt(5, p.getESizeDelta());
      s.addBatch();
    }
    int[] r = s.executeBatch();
    s.closeOnCompletion();

    return r;

  }

  private static String createPutPartitionQuery(Partition p) {
    short pid = p.getId();
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
