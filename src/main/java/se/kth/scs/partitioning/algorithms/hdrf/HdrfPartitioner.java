package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.PartitionState;

/**
 * This class is an implementation of the HDRF partitioning algorithm. Paper:
 * Petroni, Fabio, et al. "HDRF: Stream-Based Partitioning for Power-Law
 * Graphs."
 *
 * @author Hooman
 */
public class HdrfPartitioner {

    public static final double DEFAULT_LAMBDA = 1;
    public static final double DEFAULT_EPSILON = 1;

    public static void partitionWithWindow(
            PartitionState hState, Set<Tuple3<Long, Long, Double>> edges[],
            double lambda,
            double epsilon,
            int windowSize,
            int minDelay,
            int maxDelay) {
        System.out.println("Starts partitioning...");
        int nTasks = edges.length;
        HdrfPartitionerTask[] tasks = new HdrfPartitionerTask[nTasks];
        Thread[] threads = new Thread[nTasks];
        for (int i = 0; i < nTasks; i++) {
            tasks[i] = new HdrfPartitionerTask(hState, edges[i], lambda, epsilon, windowSize, minDelay, maxDelay);
            threads[i] = new Thread(tasks[i]);
        }

        long start = System.currentTimeMillis();
        System.out.println("Start Running Tasks!");
        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ex) {
                System.out.println(ex.getMessage());
            }
        }

        System.out.println(String.format("******** Partitioning finished in %d seconds **********", (System.currentTimeMillis() - start) / 1000));
    }
}
