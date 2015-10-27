package se.kth.scs.partitioning.algorithms;

import com.mysql.jdbc.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Hooman
 */
public class HdrfMysqlState implements PartitionState {

    private final Connection con; // TODO: Support for multiple connections, one per thread.
    private final int k; // Number of partitions. The partition ID must be from 0 up to k.

    public HdrfMysqlState(int k, int nConnections, String dbUrl, String dbUser, String dbPass, boolean clearDb) throws SQLException {
        this.k = k;
        con = (Connection) DriverManager.getConnection(
                String.format("%s?user=%s&password=%s", dbUrl, dbUser, dbPass));
//        con.setAutoCommit(false);
        if (clearDb) {
            HdrfMySqlQueries.clearAllTables(con);
            List<Partition> partitions = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                Partition p = new Partition(i);
                partitions.add(p);
            }
            HdrfMySqlQueries.putPartitions(partitions, con);
//            con.commit();
        }
    }

    @Override
    public int getNumberOfPartitions() {
        return k;
    }

    @Override
    public void applyState() {
        try {
            con.commit();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void releaseResources() {
        try {
            con.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Vertex getVertex(long vid) {
        Vertex v = null;
        try {
            v = HdrfMySqlQueries.getVertex(vid, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return v;
    }

    @Override
    public Map<Long, Vertex> getVertices(Set<Long> vids) {
        Map<Long, Vertex> vertices = null;
        try {
            vertices = HdrfMySqlQueries.getVertices(vids, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return vertices;
    }

    @Override
    public Map<Long, Vertex> getAllVertices() {
        Map<Long, Vertex> vertices = null;
        try {
            vertices = HdrfMySqlQueries.getAllVertices(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return vertices;
    }

    @Override
    public void putVertex(Vertex v) {
        try {
            HdrfMySqlQueries.putVertex(v, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void putVertices(Collection<Vertex> vs) {
        try {
            HdrfMySqlQueries.putVertices(vs, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Partition getPartition(int pid) {
        Partition p = null;
        try {
            p = HdrfMySqlQueries.getPartition(pid, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return p;
    }

    @Override
    public List<Partition> getPartions(int[] pids) {
        List<Partition> partitions = null;
        try {
            partitions = HdrfMySqlQueries.getPartitions(pids, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return partitions;
    }

    @Override
    public List<Partition> getAllPartitions() {
        List<Partition> partitions = null;
        try {
            partitions = HdrfMySqlQueries.getAllPartitions(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return partitions;
    }

    @Override
    public void putPartition(Partition p) {
        try {
            HdrfMySqlQueries.putPartition(p, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void putPartitions(List<Partition> p) {
        try {
            HdrfMySqlQueries.putPartitions(p, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
