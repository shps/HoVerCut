package utils;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;

/**
 *
 * @author Hooman
 */
public class InputCommands {

    public final static String HDRF = "hdrf";
    public final static String IN_MEMORY = "memory";
    public final static String MYSQL = "mysql";

//    @Parameter
//    private List<String> parameters = new ArrayList<>();
    @Parameter(names = {"-file", "-f"}, description = "Directoy of the graph file.", required = true)
    public String file;

    @Parameter(names = {"-window", "-w"}, description = "Window size.", required = true)
    public int window;

    @Parameter(names = {"-method", "-m"}, description = "Partitioning method.", validateWith = PartitionerValidator.class, required = true)
    public String method;

    @Parameter(names = {"-lambda"}, description = "Lambda value for HDRF strategy.")
    public double lambda = 1;

    @Parameter(names = {"-epsilon"}, description = "Epsilon value for HDRF strategy.")
    public double epsilon = 1;

    @Parameter(names = {"-partitions", "-p"}, description = "Number of partitions.", required = true)
    public int nPartitions = 1;

    @Parameter(names = {"-tasks", "-t"}, description = "Number of tasks (threads).", required = true)
    public int nTasks = 1;

    @Parameter(names = {"-storage", "-s"}, description = "State storage type.", validateWith = StateStorageValidator.class, required = true)
    public String storage;

    @Parameter(names = {"-db"}, description = "Database URL.")
    public String dbUrl;

    @Parameter(names = {"-user"}, description = "Database user.")
    public String user;

    @Parameter(names = {"-pass"}, description = "Database password.")
    public String pass = "";

    @Parameter(names = {"-reset"}, description = "Reset storage.", arity = 1)
    public boolean reset = true;

    @Parameter(names = {"-output"}, description = "Output file.")
    public String output = "";

    @Parameter(names = {"-append"}, description = "Append to the output file.", arity = 1)
    public boolean append = true;
    
    @Parameter(names = {"-delay"}, description = "Delay to add after every transaction with storage.", arity = 2)
    public List<Integer> delay;
    

    public static class PartitionerValidator implements IParameterValidator {

        @Override
        public void validate(String name, String value) throws ParameterException {
            switch (value) {
                case HDRF:
                    break;
                default:
                    throw new ParameterException(String.format("Partitioning method %s is not supported!", value));

            }
        }
    }

    public static class StateStorageValidator implements IParameterValidator {

        @Override
        public void validate(String name, String value) throws ParameterException {
            switch (value) {
                case IN_MEMORY:
                    break;
                case MYSQL:
                    break;
                default:
                    throw new ParameterException(String.format("Partitioning method %s is not supported!", value));

            }
        }
    }
}
