package se.kth.scs;

import java.util.Objects;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;

/**
 *
 * @author Hooman
 */
public class Test {

  public static void main(String[] args) throws Exception {
    String file = "./data/out.lkml-reply";

    // set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple3<Long, Long, Double>> replies = env.readCsvFile(file)
        .fieldDelimiter("\t")
        .ignoreComments("%")
        .includeFields("tttf")
        .types(Long.class, Long.class, Double.class);

    DataSet<Tuple3<Long, Long, Double>> filteredReplies = replies.filter(new SelfRepliesFilter());
    DataSet<Tuple3<Long, Long, Double>> distinctReplies = filteredReplies.distinct();

    Graph<Long, Double, Double> graph = Graph.fromTupleDataSet(distinctReplies, new InitVertices(), env);
    System.out.println(graph.getVertices().count());

  }

  public static final class SelfRepliesFilter implements FilterFunction<Tuple3<Long, Long, Double>> {

    @Override
    public boolean filter(Tuple3<Long, Long, Double> tuple) {
      return Objects.equals(tuple.f0, tuple.f1);
    }
  }

  public static final class InitVertices implements MapFunction<Long, Double> {

    @Override
    public Double map(Long vertexId) {
      return 1.0;

    }
  }
}
