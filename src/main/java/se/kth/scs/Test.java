package se.kth.scs;

import java.util.Collection;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.PartitionsStatistics;
import se.kth.scs.partitioning.algorithms.HdrfInMemoryState;
import se.kth.scs.partitioning.algorithms.HdrfMySqlQueries;
import se.kth.scs.partitioning.algorithms.HdrfMysqlState;
import se.kth.scs.partitioning.algorithms.HdrfPartitioner;
import se.kth.scs.partitioning.algorithms.PartitionState;
import utils.EdgeFileReader;

/**
 *
 * @author Hooman
 */
public class Test {

    public static void main(String[] args) throws Exception {
        String file = "./data/datasets/Cit-HepTh.txt";
        boolean flink = false;
        int windowSize = 10000;
        double lambda = 1;
        double epsilon = 1;
        final int k = 4;
        final int nTasks = 6;
        // set up the execution environment
        EdgeFileReader reader = new EdgeFileReader();
//        Set<Tuple3<Long, Long, Double>> edges = reader.read(file);
        Set<Tuple3<Long, Long, Double>>[] splits = reader.readSplitFile(file, nTasks);

//        if (flink) {
//            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//            DataSet<Tuple3<Long, Long, Double>> dataset = env.fromCollection(edges);
////            edges = dataset.collect();
//        }
        // Partition graph.
        // number of partitions
//        URState urState = new URState(k);
//        UniformRandomPartitioner.partition(urState, edges);
//        printResults(k, urState, String.format("UinformRandomPartitioner"));
        PartitionState hState = new HdrfInMemoryState(k);
//        PartitionState hState = new HdrfMysqlState(k, 1, HdrfMySqlQueries.DEFAULT_DB_URL, HdrfMySqlQueries.DEFAULT_USER, HdrfMySqlQueries.DEFAULT_PASS, true);
        HdrfPartitioner.partitionWithWindow(hState, splits, lambda, epsilon, windowSize);
//        HdrfPartitioner.partition(hState, edges, lambda, epsilon);

//        printResults(k, uPartitions, "UniformRandomPartitioner");
        printResults(k, hState, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", lambda, epsilon));
        hState.releaseResources();

    }

    private static void printResults(int k, PartitionState state, String message) {
        PartitionsStatistics ps = new PartitionsStatistics(state);
        System.out.println("*********** Statistics ***********");
        System.out.println(message);
        System.out.println("RF: Replication Factor.");
        System.out.println("LRSD: Load Relative Standard Deviation");
        System.out.println("MEC: Max Edge Cardinality.");
        System.out.println("MVC: Max Vertex Cardinality.");
        System.out.println(String.format("RF=%f\tLRSD=%f\tMEC=%d\tMVC=%d",
                ps.replicationFactor(),
                ps.loadRelativeStandardDeviation(),
                ps.maxEdgeCardinality(),
                ps.maxVertexCardinality()));
    }

    public static final class InitVertices implements MapFunction<Long, Double> {

        @Override
        public Double map(Long vertexId) {
            return 1.0;

        }
    }
}
