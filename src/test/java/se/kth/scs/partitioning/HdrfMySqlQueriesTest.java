//package se.kth.scs.partitioning;
//
//import se.kth.scs.partitioning.algorithms.hdrf.HdrfMySqlQueries;
//import com.mysql.jdbc.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import junit.framework.TestCase;
//
///**
// *
// * @author Ganymedian
// */
//public class HdrfMySqlQueriesTest extends TestCase {
//
//    public HdrfMySqlQueriesTest(String testName) {
//        super(testName);
//    }
//
//    /**
//     * Test of addVertexPartition method, of class HdrfMySqlQueries.
//     *
//     * @throws java.lang.Exception
//     */
//    public void testAddVertexAndGetVertices() throws Exception {
//
//        try (Connection con = init()) {
//            Vertex v1 = new Vertex(1, 0);
//            v1.addPartition(1);
//            HdrfMySqlQueries.putVertex(v1, con);
//            Vertex stored = HdrfMySqlQueries.getVertex(v1.getId(), con);
//            assert stored != null;
//            assert stored.getId() == v1.getId();
//            assert stored.getPartitions() == v1.getPartitions();
//
//            v1.addPartition(2);
//            v1.incrementDegree();
//            HdrfMySqlQueries.putVertex(v1, con);
//            stored = HdrfMySqlQueries.getVertex(v1.getId(), con);
//            assert stored != null;
//            assert stored.getId() == v1.getId();
//            assert stored.getPartitions() == v1.getPartitions();
//            assert stored.getpDegree() == v1.getpDegree() && stored.getpDegree() == 1;
//
//            Vertex v2 = new Vertex(2, 0);
//            Vertex v3 = new Vertex(3, 0);
//            Collection<Vertex> vertices = new HashSet<>();
//            vertices.add(v2);
//            vertices.add(v3);
//            HdrfMySqlQueries.putVertices(vertices, con);
//            Set<Long> vids = new HashSet();
//            vids.add(v2.getId());
//            vids.add(v3.getId());
//            Map<Long, Vertex> vMap = HdrfMySqlQueries.getVertices(vids, con);
//            assert vMap.size() == 2;
//            assert vMap.containsKey(v2.getId());
//            assert vMap.containsKey(v3.getId());
//
//            v2.addPartition(1);
//            v3.addPartition(2);
//            vertices.clear();
//            vertices.add(v2);
//            vertices.add(v3);
//            
//                HdrfMySqlQueries.putVertices(vertices, con);
//
//                vids.clear();
//                vids.add(v2.getId());
//                vids.add(v3.getId());
//                vMap = HdrfMySqlQueries.getVertices(vids, con);
//                assert vMap.size() == 2;
//                assert vMap.containsKey(v2.getId());
//                assert vMap.get(v2.getId()).containsPartition(1);
//                assert vMap.containsKey(v3.getId());
//                assert vMap.get(v3.getId()).containsPartition(2);
//            
//        }
//    }
//
//    private Connection init() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
//        Class.forName(HdrfMySqlQueries.JDBC_DRIVER).newInstance();
//        Connection con = (Connection) DriverManager.getConnection(
//                String.format("%s?user=%s&password=%s", HdrfMySqlQueries.DEFAULT_DB_URL, HdrfMySqlQueries.DEFAULT_USER, HdrfMySqlQueries.DEFAULT_PASS));
//        HdrfMySqlQueries.clearAllTables(con);
//        return con;
//    }
//
//    /**
//     * Test of incrementPartitionSize method, of class HdrfMySqlQueries.
//     *
//     * @throws java.lang.Exception
//     */
//    public void testIncrementPartitionSize() throws Exception {
//        try (Connection con = init()) {
//            Partition p1 = new Partition(1);
//            HdrfMySqlQueries.putPartition(p1, con);
//            Partition storedP1 = HdrfMySqlQueries.getPartition(p1.getId(), con);
//            assert storedP1 != null;
//            assert storedP1.getId() == p1.getId();
//            p1.incrementESize();
//            p1.incrementVSize();
//            HdrfMySqlQueries.putPartition(p1, con);
//            storedP1 = HdrfMySqlQueries.getPartition(p1.getId(), con);
//            assert storedP1 != null;
//            assert storedP1.getId() == p1.getId();
//            assert storedP1.getVSize() == p1.getVSize();
//            assert storedP1.getESize() == p1.getESize();
//            HdrfMySqlQueries.putPartition(p1, con);
//            Partition p2 = new Partition(2);
//            Partition p3 = new Partition(3);
//            List<Partition> ps = new ArrayList(2);
//            ps.add(p2);
//            ps.add(p3);
//            HdrfMySqlQueries.putPartitions(ps, con);
//            List<Partition> partitions = HdrfMySqlQueries.getPartitions(new int[]{p2.getId(), p3.getId()}, con);
//            assert partitions.size() == 2;
//            assert partitions.contains(p2);
//            assert partitions.contains(p3);
//            for (Partition p : partitions) {
//                assert p.getESize() == 0;
//                assert p.getVSize() == 0;
//            }
//        }
//    }
//}
