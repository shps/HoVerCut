package se.kth.scs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionsStatistics;
import se.kth.scs.partitioning.algorithms.HdrfPartitioner;
import se.kth.scs.partitioning.algorithms.UniformRandomPartitioner;
import utils.EdgeFileReader;

/**
 *
 * @author Hooman
 */
public class Test {

    public static void main(String[] args) throws Exception {
        String file = "./data/Cit-HepTh.txt";

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, Long, Double>> citation = env.readCsvFile(file)
                .fieldDelimiter("\t")
                .ignoreComments("#")
                .includeFields("tt")
                .types(Long.class, Long.class, Double.class);

        Graph<Long, Double, Double> graph = Graph.fromTupleDataSet(citation, new InitVertices(), env);
        long n = graph.getVertices().count();
        long m = graph.getEdges().count();
//    List<Tuple3<Long, Long, Double>> edges = citation.collect();
        List<Tuple3<Long, Long, Double>> edges = new ArrayList<>(new EdgeFileReader().read(file));

        // Partition graph.
        final int k = 4; // number of partitions
        Partition[] uPartitions = UniformRandomPartitioner.partition(edges, k);

        double lambda = 1;
        double epsilon = 1;
        Partition[] hPartitions = HdrfPartitioner.partition(edges, k, lambda, epsilon);

        printResults(n, m, k, uPartitions, "UniformRandomPartitioner");
        printResults(n, m, k, hPartitions, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", lambda, epsilon));

    }

    private static void printResults(long n, long m, int k, Partition[] partitions, String message) {
        PartitionsStatistics ps = new PartitionsStatistics(partitions);
        System.out.println("*********** Statistics ***********");
        System.out.println(message);
        System.out.println(String.format("N=%d\t M=%d", n, m));
        for (int i = 0; i < k; i++) {
            System.out.println(String.format("Partition %d\tn=%d\tm=%d",
                    i,
                    partitions[i].vertexSize(),
                    partitions[i].edgeSize()));
        }
        System.out.println(String.format("RF=%f\tLRSD=%f\tMEC=%d\tMVC=%d",
                ps.replicationFactor(),
                ps.loadRelativeStandardDeviation(),
                ps.maxEdgeCardinality(),
                ps.maxVertexCardinality()));
    }

//  public static final class SelfRepliesFilter implements FilterFunction<Tuple3<Long, Long, Double>> {
//
//    @Override
//    public boolean filter(Tuple3<Long, Long, Double> tuple) {
//      return Objects.equals(tuple.f0, tuple.f1);
//    }
//  }
    public static final class InitVertices implements MapFunction<Long, Double> {

        @Override
        public Double map(Long vertexId) {
            return 1.0;

        }
    }
}
