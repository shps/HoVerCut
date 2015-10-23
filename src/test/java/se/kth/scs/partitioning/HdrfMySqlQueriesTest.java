package se.kth.scs.partitioning;

import se.kth.scs.partitioning.algorithms.HdrfMySqlQueries;
import com.mysql.jdbc.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import junit.framework.TestCase;

/**
 *
 * @author Ganymedian
 */
public class HdrfMySqlQueriesTest extends TestCase {

    public HdrfMySqlQueriesTest(String testName) {
        super(testName);
    }

    /**
     * Test of addVertexPartition method, of class HdrfMySqlQueries.
     */
    public void testAddVertexAndGetVertices() throws Exception {

        try (Connection con = init()) {
            long vid = 1;
            long vid2 = 2;
            int pid = 1;
            HdrfMySqlQueries.addVertexPartition(vid, pid, con);
            HdrfMySqlQueries.addVertexPartition(vid2, pid, con);
            con.commit();

            Long[] vs = HdrfMySqlQueries.getVerticesForPartition(pid, con);
            assert vs.length == 2;
            assert (vs[0] == vid && vs[1] == vid2) || (vs[0] == vid2 && vs[1] == vid);
            assert HdrfMySqlQueries.getVertexSizeForPartition(pid, con) == 2;
        }
    }

    private Connection init() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        Class.forName(HdrfMySqlQueries.JDBC_DRIVER).newInstance();
        Connection con = (Connection) DriverManager.getConnection(
                String.format("%s?user=%s&password=%s", HdrfMySqlQueries.DEFAULT_DB_URL, HdrfMySqlQueries.DEFAULT_USER, HdrfMySqlQueries.DEFAULT_PASS));
        con.setAutoCommit(false);
        HdrfMySqlQueries.clearAllTables(con);
        con.commit();
        return con;
    }

    /**
     * Test of incrementPartitionSize method, of class HdrfMySqlQueries.
     */
    public void testIncrementPartitionSize() throws Exception {
        try (Connection con = init()) {
            HdrfMySqlQueries.incrementPartitionSize(1, con);
            HdrfMySqlQueries.incrementPartitionSize(1, con);
            con.commit();

            assert HdrfMySqlQueries.getEdgeSizeForPartition(1, con) == 2;
        }

    }

    /**
     * Test of incrementVertexDegree method, of class HdrfMySqlQueries.
     */
    public void testIncrementVertexDegree() throws Exception {

        try (Connection con = init()) {
            HdrfMySqlQueries.incrementVertexDegree(1, con);
            HdrfMySqlQueries.incrementVertexDegree(1, con);
            con.commit();

            assert HdrfMySqlQueries.getVertexDegree(1, con) == 2;
        }
    }

    /**
     * Test of partitionContainsVertex method, of class HdrfMySqlQueries.
     */
    public void testPartitionContainsVertex() throws Exception {
    }

    public void testMaxMinPartitionSize() throws Exception {
        try (Connection con = init()) {
            HdrfMySqlQueries.incrementPartitionSize(1, con);
            HdrfMySqlQueries.incrementPartitionSize(1, con);
            HdrfMySqlQueries.incrementPartitionSize(2, con);
            con.commit();

            assert HdrfMySqlQueries.getMaxPartitionEdgeSize(con) == 2;
            assert HdrfMySqlQueries.getMinPartitionEdgeSize(con) == 1;
        }
    }

    public void testTotalSizes() throws Exception {
        try (Connection con = init()) {
            HdrfMySqlQueries.incrementVertexDegree(1, con);
            HdrfMySqlQueries.incrementVertexDegree(2, con);
            con.commit();

            assert HdrfMySqlQueries.getTotalNumberOfVertices(con) == 2;

            HdrfMySqlQueries.addVertexPartition(1, 1, con);
            HdrfMySqlQueries.addVertexPartition(2, 2, con);
            HdrfMySqlQueries.addVertexPartition(2, 1, con);
            con.commit();

            assert HdrfMySqlQueries.getTotalNumberOfReplicas(con) == 3;

            HdrfMySqlQueries.incrementPartitionSize(1, con);
            HdrfMySqlQueries.incrementPartitionSize(1, con);
            HdrfMySqlQueries.incrementPartitionSize(2, con);
            con.commit();

            assert HdrfMySqlQueries.getTotalNumberOfEdges(con) == 3;

        }
    }

}
