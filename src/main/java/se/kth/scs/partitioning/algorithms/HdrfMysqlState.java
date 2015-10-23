package se.kth.scs.partitioning.algorithms;

import com.mysql.jdbc.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * @author Hooman
 */
public class HdrfMysqlState implements HdrfState {

    private final Connection con; // TODO: Support for multiple connections, one per thread.
    private final int k; // Number of partitions. The partition ID must be from 0 up to k.

    public HdrfMysqlState(int k, int nConnections, String dbUrl, String dbUser, String dbPass, boolean clearDb) throws SQLException {
        this.k = k;
        con = (Connection) DriverManager.getConnection(
                String.format("%s?user=%s&password=%s", dbUrl, dbUser, dbPass));
        con.setAutoCommit(false);
        if (clearDb) {
            HdrfMySqlQueries.clearAllTables(con);
            con.commit();
        }
    }

    @Override
    public int updateVertexDegree(long v) {
        int degree = 0;
        try {
            HdrfMySqlQueries.incrementVertexDegree(v, con);
            degree = HdrfMySqlQueries.getVertexDegree(v, con);
        } catch (SQLException ex) {
            // TODO: logging
            ex.printStackTrace();
        }

        return degree;
    }

    @Override
    public void addEdgeToPartition(int p, Tuple3<Long, Long, Double> edge) {
        try {
            HdrfMySqlQueries.addVertexPartition(edge.f0, p, con);
            HdrfMySqlQueries.addVertexPartition(edge.f1, p, con);
            HdrfMySqlQueries.incrementPartitionSize(p, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public int getMaxPartitionEdgeSize() {
        try {
            return HdrfMySqlQueries.getMaxPartitionEdgeSize(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    @Override
    public int getMinPartitionEdgeSize() {
        try {
            return HdrfMySqlQueries.getMinPartitionEdgeSize(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

//    @Override
//    public Partition[] getPartitions() {
//        Partition[] partitions = new Partition[k];
//        for (int i = 0; i < k; i++)
//        {
//            partitions[i] = new Partition(i);
//            try {
//                partitions[i].setEdgeSize(HdrfMySqlQueries.getEdgeSizeForPartition(i, con));
//            } catch (SQLException ex) {
//                Logger.getLogger(HdrfMysqlState.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//    }
    @Override
    public boolean partitionContainsVertex(int p, long v) {
        try {
            return HdrfMySqlQueries.partitionContainsVertex(p, v, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return false;
    }

    @Override
    public int getPartitionEdgeSize(int p) {
        try {
            return HdrfMySqlQueries.getEdgeSizeForPartition(p, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    @Override
    public int getPartitionVertexSize(int p) {
        try {
            return HdrfMySqlQueries.getVertexSizeForPartition(p, con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    @Override
    public int getMaxPartitionVertexSize() {
        try {
            return HdrfMySqlQueries.getMaxPartitionVertexSize(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    @Override
    public int getTotalNumberOfReplicas() {
        try {
            return HdrfMySqlQueries.getTotalNumberOfReplicas(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    @Override
    public int getTotalNumberOfVertices() {
        try {
            return HdrfMySqlQueries.getTotalNumberOfVertices(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    @Override
    public int getTotalNumberOfEdges() {
        try {
            return HdrfMySqlQueries.getTotalNumberOfEdges(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return 0;
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

}
