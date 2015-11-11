package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.PartitionsStatistics;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfInMemoryState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfMysqlState;
import se.kth.scs.partitioning.algorithms.hdrf.HdrfPartitioner;
import utils.EdgeFileReader;
import utils.InputCommands;

/**
 *
 * @author Hooman
 */
public class GraphPartitioner {

    private static final Map<Integer, List<Float>> windowRf = new LinkedHashMap<>();
    private static final Map<Integer, List<Float>> taskRf = new LinkedHashMap<>();
    private static final Map<Integer, List<Integer>> windowTime = new LinkedHashMap<>();
    private static final Map<Integer, List<Integer>> taskTime = new LinkedHashMap<>();
    private static final String DELIMITER = " ";
    private static final String storage = "memory";
  private static final String dbUrl = "jdbc:mysql://130.211.66.108:3306/hdrf";
   private static final String outputFile = "/home/ganymedian/Desktop/results/hdrf";
  private static final String inputFile = "./data/datasets/twitter_combined.txt";
    private static final int minT = 0;
    private static final int maxT = 0;
    private static final int tBase = 2;
    private static final int minW = 0;
    private static final int maxW = 0;
    private static final int wBase = 10;
//  private static final int maxW = 5;
    private static final String USER = "root";
    private static final String PASS = "root";

    public static void main(String[] args) throws SQLException {
//        args = new String[]{
//            "-f", "./data/datasets/Cit-HepTh.txt",
//            "-w", "1000",
//            "-m", "hdrf",
//            "-p", "4",
//            "-t", "4",
//            //            "-reset", "true",
//            "-s", "memory",
//            "-db", "jdbc:mysql://localhost/hdrf",
//            "-user", "root",
//            "-pass", "",
//            "-output", "/Users/Ganymedian/Desktop/hdrf",
//            "-append", "true",
//            "-delay", "10", "20"};
        int wb = wBase;
        int p = 4;
        int tb = tBase;

        for (int i = minT; i <= maxT; i++) {
            int t = (int) Math.pow(tb, i);
            int j = minW;
            int w;
            System.out.println(String.format("Reading file %s", inputFile));
            long start = System.currentTimeMillis();
            EdgeFileReader reader = new EdgeFileReader(DELIMITER);
            Set<Tuple3<Long, Long, Double>>[] splits = reader.readSplitFile(inputFile, t);
            int nEdges = reader.getnEdges();
            System.out.println(String.format("Finished reading in %d seconds.", (System.currentTimeMillis() - start) / 1000));
            while (true) {
                w = (int) Math.pow(wb, j);
                if (w * t > nEdges) {
                    break;
                } else if ((maxW >= 0) && (j > maxW)) {
                    break;
                }
                InputCommands commands = new InputCommands();
                JCommander commander;
                try {
                    args = new String[]{
                        "-f", inputFile,
                        "-w", String.valueOf(w),
                        "-m", "hdrf",
                        "-p", "4",
                        "-t", String.valueOf(t),
                        "-reset", "true",
                        "-s", storage,
                        "-db", dbUrl,
                        "-user", USER,
                        "-pass", PASS,
                        "-output", outputFile,
                        "-append", "true",
                        "-delay", "0", "0"};
                    commander = new JCommander(commands, args);
                    runPartitioner(commands, splits);
                } catch (ParameterException ex) {
                    System.out.println(ex.getMessage());
                    System.out.println(Arrays.toString(args));
                    commander = new JCommander(commands);
                    commander.usage();
                    System.out.println(String.format("A valid command is like: %s",
                            "-f ./data/datasets/Cit-HepTh.txt -w 1000 -m hdrf -p 4 -t 4 -s mysql -db jdbc:mysql://localhost/hdrf -user root"));
                }
                j++;
            }
        }

        writeToFile(windowRf, taskRf, windowTime, taskTime, outputFile);

    }

    public static void runPartitioner(InputCommands commands, Set<Tuple3<Long, Long, Double>>[] splits) throws SQLException {
        int minDelay = 0;
        int maxDelay = 0;
        if (commands.nTasks > 1 && commands.delay != null && commands.delay.size() > 0) {
            if (commands.delay.get(0) > commands.delay.get(1)) {
                throw new ParameterException("delay max time cannot be less than min time!");
            }

            minDelay = commands.delay.get(0);
            maxDelay = commands.delay.get(1);

        }
        printCommandSetup(commands);
        if (!commands.method.equals(InputCommands.HDRF)) {
            throw new ParameterException("");
        }
        PartitionState state = null;
        switch (commands.storage) {
            case InputCommands.IN_MEMORY:
                state = new HdrfInMemoryState(commands.nPartitions);
                break;
            case InputCommands.MYSQL:
                state = new HdrfMysqlState(
                        commands.nPartitions,
                        commands.dbUrl,
                        commands.user,
                        commands.pass,
                        commands.reset);
                break;
            default:
                throw new ParameterException("");
        }

        long start = System.currentTimeMillis();
        HdrfPartitioner.partitionWithWindow(state, splits, commands.lambda, commands.epsilon, commands.window, minDelay, maxDelay);
        int duration = (int) ((System.currentTimeMillis() - start) / 1000);
        PartitionsStatistics ps = new PartitionsStatistics(state);
        printResults(commands.nPartitions, ps, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", commands.lambda, commands.epsilon));
        if (!commands.output.isEmpty()) {
            try {
                GraphPartitioner.writeToFile(ps, commands);
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }
        }

        if (!windowRf.containsKey(commands.window)) {
            windowRf.put(commands.window, new LinkedList<Float>());
        }
        windowRf.get(commands.window).add(ps.replicationFactor());
        if (!taskRf.containsKey(commands.nTasks)) {
            taskRf.put(commands.nTasks, new LinkedList<Float>());
        }
        taskRf.get(commands.nTasks).add(ps.replicationFactor());
        if (!windowTime.containsKey(commands.window)) {
            windowTime.put(commands.window, new LinkedList<Integer>());
        }
        windowTime.get(commands.window).add(duration);
        if (!taskTime.containsKey(commands.nTasks)) {
            taskTime.put(commands.nTasks, new LinkedList<Integer>());
        }
        taskTime.get(commands.nTasks).add(duration);
        state.releaseResources();
    }

    private static void printResults(int k, PartitionsStatistics ps, String message) {
        System.out.println("*********** Statistics ***********");
        System.out.println(message);
        System.out.println(String.format("Partitions:\t%d", k));
        System.out.println(String.format("Vertices:\t%d", ps.getNVertices()));
        System.out.println(String.format("Edges:\t%d", ps.getNEdges()));
        int[] vp = ps.getNVerticesPartitions();
        int[] ep = ps.getNEdgesPartitions();
        for (int i = 0; i < vp.length; i++) {
            System.out.println(String.format("P%d:\tv=%d\te=%d", i, vp[i], ep[i]));
        }
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

    private static void printCommandSetup(InputCommands commands) {
        final String newLine = "\n";
        StringBuilder sb = new StringBuilder("Your partitionig configurations:\n");
        sb.append("file:\t").append(commands.file).append(newLine);
        sb.append("window:\t").append(commands.window).append(newLine);
        sb.append("method:\t").append(commands.method).append(newLine);
        sb.append("partitions:\t").append(commands.nPartitions).append(newLine);
        sb.append("tasks(threads):\t").append(commands.nTasks).append(newLine);

        if (commands.storage.contentEquals(InputCommands.HDRF)) {
            sb.append("lambda:\t").append(commands.lambda).append(newLine);
            sb.append("epsilon:\t").append(commands.epsilon).append(newLine);
        }
        sb.append("storage:\t").append(commands.storage).append(newLine);
        sb.append("reset storage:\t").append(commands.reset).append(newLine);
        if (commands.storage.contentEquals(InputCommands.MYSQL)) {
            sb.append("db:\t").append(commands.dbUrl).append(newLine);
            sb.append("db user:\t").append(commands.user).append(newLine);
            sb.append("db pass:\t").append(commands.pass).append(newLine);
        }
        sb.append("Output:\t").append(commands.output).append(newLine);
        sb.append(String.format("Delay:\t min:%d\tmax=%d", commands.delay.get(0), commands.delay.get(1))).append(newLine);
        sb.append("Append to output:\t").append(commands.append).append(newLine);
        System.out.println(sb.toString());
    }

    public static void writeToFile(PartitionsStatistics ps, InputCommands commands) throws FileNotFoundException {
//        File f1 = new File(commands.output + "-partitions.csv");
//        boolean append = false;
//        if (f1.exists() && !f1.isDirectory()) {
//            append = commands.append;
//        }
//        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
//                f1,
//                append))) {
//            Collection<Vertex> vertices = ps.getVertices().values();
//            for (Vertex v : vertices) {
//                writer.append(String.format("%d,", v.getId()));
//                for (int p : v.getPartitions()) {
//                    writer.append(String.format("%d,", p));
//                }
//                writer.append("\n");
//            }
//
//            writer.flush();
//        }
        boolean append = false;
        String file = commands.output + "-result.csv";
        File f2 = new File(file);
        if (f2.exists() && !f2.isDirectory()) {
            append = commands.append;
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
                f2,
                commands.append))) {
            if (!append) {
                writer.write("nTasks,nPartitions,window,rf,lrsd,mec,mvc\n");
            }
            writer.append(String.format("%d,%d,%d,%f,%f,%d,%d",
                    commands.nTasks,
                    commands.nPartitions,
                    commands.window,
                    ps.replicationFactor(),
                    ps.loadRelativeStandardDeviation(),
                    ps.maxEdgeCardinality(),
                    ps.maxVertexCardinality()));
            writer.append("\n");
            writer.flush();
        }
    }

    private static void writeToFile(
            Map<Integer, List<Float>> windowRf,
            Map<Integer, List<Float>> taskRf,
            Map<Integer, List<Integer>> windowTime,
            Map<Integer, List<Integer>> taskTime,
            String file) {
        String fName = file + "-window.csv";
        File f1 = new File(fName);
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
                f1))) {
            Iterator<Map.Entry<Integer, List<Float>>> iterator = windowRf.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, List<Float>> entry = iterator.next();
                writer.append(entry.getKey().toString()).append(",");
                for (float rf : entry.getValue()) {
                    writer.append(String.valueOf(rf)).append(",");
                }
                writer.append("\n");
            }

            writer.flush();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
        }

        fName = file + "-tasks.csv";
        f1 = new File(fName);
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
                f1))) {
            Iterator<Map.Entry<Integer, List<Float>>> iterator = taskRf.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, List<Float>> entry = iterator.next();
                writer.append(entry.getKey().toString()).append(",");
                for (float rf : entry.getValue()) {
                    writer.append(String.valueOf(rf)).append(",");
                }
                writer.append("\n");
            }

            writer.flush();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
        }

        fName = file + "-windows-time.csv";
        f1 = new File(fName);
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
                f1))) {
            Iterator<Map.Entry<Integer, List<Integer>>> iterator = windowTime.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, List<Integer>> entry = iterator.next();
                writer.append(entry.getKey().toString()).append(",");
                for (int time : entry.getValue()) {
                    writer.append(String.valueOf(time)).append(",");
                }
                writer.append("\n");
            }

            writer.flush();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
        }

        fName = file + "-tasks-time.csv";
        f1 = new File(fName);
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(
                f1))) {
            Iterator<Map.Entry<Integer, List<Integer>>> iterator = taskTime.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, List<Integer>> entry = iterator.next();
                writer.append(entry.getKey().toString()).append(",");
                for (int time : entry.getValue()) {
                    writer.append(String.valueOf(time)).append(",");
                }
                writer.append("\n");
            }

            writer.flush();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GraphPartitioner.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
