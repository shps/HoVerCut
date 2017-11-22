package se.kth.scs.partitioning.hovercut;

import java.util.LinkedHashSet;
import java.util.LinkedList;

import se.kth.scs.partitioning.Edge;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.policy.PartitionSelectionPolicy;

/**
 * This class is an implementation of HoVerCut(A Horizontally and Vertically scalable streaming graph Vertex-Cut
 * partitioner). To implement HoVerCut we inspire a greedy heuristic from HDRF partitioning algorithm that is published
 * as: Petroni, Fabio, et al. "HDRF: Stream-Based Partitioning for Power-Law Graphs."
 *
 * @author Hooman
 */
public class HovercutPartitioner {

    public static final double DEFAULT_LAMBDA = 1;
    public static final double DEFAULT_EPSILON = 1;

    /**
     * @param hState
     * @param edges
     * @param heuristic
     * @param windowSize
     * @param pUpdateFrequency
     * @param exactDegree
     * @return
     */
    public static LinkedList<Edge>[] partitionWithWindow(
            PartitionState hState, LinkedHashSet<Edge> edges[],
            PartitionSelectionPolicy heuristic,
            int windowSize,
            int pUpdateFrequency,
            boolean exactDegree) {
        System.out.println("Starts partitioning...");
        int nTasks = edges.length;
        Subpartitioner[] tasks = new Subpartitioner[nTasks];
        Thread[] threads = new Thread[nTasks];
        for (int i = 0; i < nTasks; i++) {
            tasks[i] = new Subpartitioner(
                    hState,
                    edges[i],
                    heuristic,
                    windowSize,
                    pUpdateFrequency,
                    exactDegree);
            threads[i] = new Thread(tasks[i]);
        }

        long start = System.currentTimeMillis();
        executeThreads(threads);
        System.out.println(String.format("******** Partitioning finished in %d seconds **********", (System.currentTimeMillis() - start) / 1000));

//    LinkedList<Edge>[][] outputAssignments = new LinkedList[nTasks][hState.getNumberOfPartitions()];
//    for (int i = 0; i < nTasks; i++) {
//      outputAssignments[i] = tasks[i].getAssignments();
//    }
        LinkedList<Edge>[] outputAssignments = new LinkedList[hState.getNumberOfPartitions()];
        for (int i = 0; i < hState.getNumberOfPartitions(); i++) {
            outputAssignments[i] = new LinkedList();
            for (Subpartitioner task : tasks) {
                outputAssignments[i].addAll(task.getAssignments()[i]);
            }
        }
//    System.out.println(String.format("******** edges after resplitting %d **********", outputAssignments.size()));
        return outputAssignments;
    }

    public static void computeExactDegrees(PartitionState hState, LinkedHashSet<Edge> edges[]) {
        int nTasks = edges.length;
        ExactDegreeTask[] tasks = new ExactDegreeTask[nTasks];
        Thread[] threads = new Thread[nTasks];
        for (int i = 0; i < nTasks; i++) {
            tasks[i] = new ExactDegreeTask(hState, edges[i]);
            threads[i] = new Thread(tasks[i]);
        }
        executeThreads(threads);
    }

    private static void executeThreads(Thread[] threads) {
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
    }
}
