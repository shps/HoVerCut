package se.kth.scs.partitioning.hovercut;

import com.mysql.jdbc.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class HovercutMysqlState implements PartitionState {

//    private final Connection con; // TODO: Support for multiple connections, one per thread.
  private final short k; // Number of partitions. The partition ID must be from 0 up to k.
  private final String dbUrl;
  private final String dbUser;
  private final String dbPass;
  private final ThreadLocal<Connection> cons = new ThreadLocal<>();

  public HovercutMysqlState(short k, String dbUrl, String dbUser, String dbPass, boolean clearDb) throws SQLException {
    this.k = k;
    this.dbUrl = dbUrl;
    this.dbUser = dbUser;
    this.dbPass = dbPass;
    Connection con = createConnection(dbUrl, dbUser, dbPass);
//        con.setAutoCommit(false);
    if (clearDb) {
      HovercutMySqlQueries.clearAllTables(con);
      List<Partition> partitions = new ArrayList<>(k);
      for (short i = 0; i < k; i++) {
        Partition p = new Partition(i);
        partitions.add(p);
      }
      HovercutMySqlQueries.putPartitions(partitions, con);
      con.close();
//            con.commit();
    }
  }

  private Connection createConnection(String dbUrl, String dbUser, String dbPass) throws SQLException {
//        + "cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&"
//                        + "useUnbufferedIO=false&useReadAheadInput=false"
    return (Connection) DriverManager.getConnection(
      String.format("%s?user=%s&password=%s&rewriteBatchedStatements=true", dbUrl, dbUser, dbPass));
  }

  private Connection getConnection() throws SQLException {
    Connection con = cons.get();
    if (con == null) {
      con = createConnection(dbUrl, dbUser, dbPass);
      cons.set(con);
    }

    return con;
  }

  @Override
  public short getNumberOfPartitions() {
    return k;
  }

  @Override
  public void applyState() {
    try {
      Connection con = getConnection();
      con.commit();
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void releaseResources(boolean clearAll) {
    // TODO: implement clearAll.
    // No overall resource is used.
  }

  @Override
  public Vertex getVertex(int vid) {
    Vertex v = null;
    try {
      Connection con = getConnection();
      v = HovercutMySqlQueries.getVertex(vid, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    return v;
  }

  @Override
  public Map<Integer, Vertex> getVertices(Set<Integer> vids) {
    Map<Integer, Vertex> vertices = null;
    try {
      Connection con = getConnection();
      vertices = HovercutMySqlQueries.getVertices(vids, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    return vertices;
  }

  @Override
  public Map<Integer, Vertex> getAllVertices(int expectedSize) {
    Map<Integer, Vertex> vertices = null;
    try {
      Connection con = getConnection();
      vertices = HovercutMySqlQueries.getAllVertices(con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    return vertices;
  }

  @Override
  public void putVertex(Vertex v) {
    try {
      Connection con = getConnection();
      HovercutMySqlQueries.putVertex(v, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void putVertices(Collection<Vertex> vs) {
    try {
      Connection con = getConnection();
      HovercutMySqlQueries.putVertices(vs, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public Partition getPartition(short pid) {
    Partition p = null;
    try {
      Connection con = getConnection();
      p = HovercutMySqlQueries.getPartition(pid, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    return p;
  }

  @Override
  public List<Partition> getPartions(short[] pids) {
    List<Partition> partitions = null;
    try {
      Connection con = getConnection();
      partitions = HovercutMySqlQueries.getPartitions(pids, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    return partitions;
  }

  @Override
  public List<Partition> getAllPartitions() {
    List<Partition> partitions = null;
    try {
      Connection con = getConnection();
      partitions = HovercutMySqlQueries.getAllPartitions(con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    return partitions;
  }

  @Override
  public void putPartition(Partition p) {
    try {
      Connection con = getConnection();
      HovercutMySqlQueries.putPartition(p, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void putPartitions(List<Partition> p) {
    try {
      Connection con = getConnection();
      HovercutMySqlQueries.putPartitions(p, con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void releaseTaskResources() {
    try {
      Connection con = getConnection();
      con.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void waitForAllUpdates(int expectedSize) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
