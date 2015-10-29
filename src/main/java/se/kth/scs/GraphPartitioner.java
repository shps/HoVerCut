package se.kth.scs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
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
//        args = new String[]{
//            "-f", "./data/datasets/Cit-HepTh.txt",
//            "-w", "1000",
//            "-m", "hdrf",
//            "-p", "4",
//            "-t", "4",
//            "-s", "mysql",
//            "-db", "jdbc:mysql://localhost/hdrf2",
//            "-user", "root",
//            "-pass", ""};
        InputCommands commands = new InputCommands();
        JCommander commander;
        try {
            commander = new JCommander(commands, args);
            printCommandSetup(commands);
            if (!commands.method.equals(InputCommands.HDRF)) {
                throw new ParameterException("");
            }
            EdgeFileReader reader = new EdgeFileReader();
            Set<Tuple3<Long, Long, Double>>[] splits = reader.readSplitFile(commands.file, commands.nTasks);
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
                            true);
                    break;
                default:
                    throw new ParameterException("");
            }

            HdrfPartitioner.partitionWithWindow(state, splits, commands.lambda, commands.epsilon, commands.window);
            printResults(commands.nPartitions, state, String.format("HdrfPartitioner lambda=%f\tepsilon=%f", commands.lambda, commands.epsilon));
            state.releaseResources();
        } catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            System.out.println(Arrays.toString(args));
            commander = new JCommander(commands);
            commander.usage();
            System.out.println(String.format("A valid command is like: %s",
                    "-f ./data/datasets/Cit-HepTh.txt -w 1000 -m hdrf -p 4 -t 4 -s mysql -db jdbc:mysql://localhost/hdrf2 -user root"));
        }

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
        if (commands.storage.contentEquals(InputCommands.MYSQL)) {
            sb.append("db:\t").append(commands.dbUrl).append(newLine);
            sb.append("db user:\t").append(commands.user).append(newLine);
            sb.append("db pass:\t").append(commands.pass).append(newLine);
        }

        System.out.println(sb.toString());
    }
}
