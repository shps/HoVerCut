package se.kth.scs;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;

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
    final HashSet<Long>[] vPartitions = new HashSet[k];
    final HashSet<Tuple3<Long, Long, Double>>[] ePartitions = new HashSet[k];
    for (int i = 0; i < k; i++) {
      vPartitions[i] = new HashSet<>();
      ePartitions[i] = new HashSet<>();
    }

    SecureRandom r = new SecureRandom();
    for (Tuple3<Long, Long, Double> e : edges) {
      int p = r.nextInt(k);
      ePartitions[p].add(e);
      vPartitions[p].add(e.f0);
      vPartitions[p].add(e.f1);
    }

    System.out.println(String.format("N=%d\t M=%d", n, m));
    for (int i = 0; i < k; i++) {
      System.out.println(String.format("Partition %d\tn=%d\tm=%d", i, vPartitions[i].size(), ePartitions[i].size()));
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
