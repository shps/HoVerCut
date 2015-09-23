package se.kth.scs;

import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.UniformRandomPartitioner;

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
    List<Tuple3<Long, Long, Double>> edges = citation.collect();

    final int k = 4; // number of partitions
    Partition[] partitions = UniformRandomPartitioner.partition(edges, k);

    System.out.println(String.format("N=%d\t M=%d", n, m));
    for (int i = 0; i < k; i++) {
      System.out.println(String.format("Partition %d\tn=%d\tm=%d", i, partitions[i].vertexSize(), partitions[i].edgeSize()));
    }
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
