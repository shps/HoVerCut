package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
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

    public static void main(String[] args) throws SQLException {
        args = new String[]{
            "-f", "./data/datasets/Cit-HepTh.txt",
            "-w", "1000",
            "-m", "hdrf",
            "-p", "4",
            "-t", "4",
            //            "-reset", "true",
            "-s", "memory",
            "-db", "jdbc:mysql://localhost/hdrf",
            "-user", "root",
            "-pass", "",
            "-output", "/Users/Ganymedian/Desktop/hdrf",
            "-append", "true"};
        InputCommands commands = new InputCommands();
        JCommander commander;
        try {
            commander = new JCommander(commands, args);
            printCommandSetup(commands);
            if (!commands.method.equals(InputCommands.HDRF)) {
                throw new ParameterException("");
            }
            System.out.println(String.format("Reading file %s", commands.file));
            long start = System.currentTimeMillis();
            EdgeFileReader reader = new EdgeFileReader();
            Set<Tuple3<Long, Long, Double>>[] splits = reader.readSplitFile(commands.file, commands.nTasks);
            System.out.println(String.format("Finished reading in %d seconds.", (System.currentTimeMillis() - start) / 1000));
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

            HdrfPartitioner.partitionWithWindow(state, splits, commands.lambda, commands.epsilon, commands.window);
            PartitionsStatistics ps = new PartitionsStatistics(state);
            printResults(commands.nPartitions, ps, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", commands.lambda, commands.epsilon));
            if (!commands.output.isEmpty()) {
                try {
                    writeToFile(ps, commands);
                } catch (FileNotFoundException ex) {
                    ex.printStackTrace();
                }
            }
            state.releaseResources();
        } catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            System.out.println(Arrays.toString(args));
            commander = new JCommander(commands);
            commander.usage();
            System.out.println(String.format("A valid command is like: %s",
                    "-f ./data/datasets/Cit-HepTh.txt -w 1000 -m hdrf -p 4 -t 4 -s mysql -db jdbc:mysql://localhost/hdrf -user root"));
        }

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
        sb.append("Append to output:\t").append(commands.append).append(newLine);
        System.out.println(sb.toString());
    }

    public static void writeToFile(PartitionsStatistics ps, InputCommands commands) throws FileNotFoundException{
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
}
