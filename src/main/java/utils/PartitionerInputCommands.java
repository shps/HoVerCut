package utils;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.List;

/**
 * This class provides all the commands available to user to run the multi-loader HDRFS application.
 *
 * @author Hooman
 */
public class PartitionerInputCommands {

  public final static String HDRF = "hdrf";
  public final static String IN_MEMORY = "memory";
  public final static String MYSQL = "mysql";
  public final static String REMOTE = "remote";

  @Parameter(names = {"-file", "-f"}, description = "Directoy of the graph file.", required = true)
  public String file;

  @Parameter(names = {"-window", "-w"}, description = "Window size: (1)base (2)min exponent (3)max exponent.", arity = 3, required = true)
  public List<Integer> window;

  @Parameter(names = {"-puf"}, description = "Partitions update frequency comparing to the window size.")
  public int partitionsUpdateFrequency = 1;

  @Parameter(names = {"-rs"}, description = "Number of restreamings.")
  public int restreaming = 0;

  @Parameter(names = {"-tasks", "-t"}, description = "Number of tasks (threads): (1)base (2)min exponent (3)max exponent.", arity = 3, required = true)
  public List<Integer> nTasks;

  @Parameter(names = {"-method", "-m"}, description = "Partitioning method.", validateWith = PartitionerValidator.class, required = true)
  public String method;

  @Parameter(names = {"-lambda"}, description = "Lambda value for HDRF strategy.")
  public double lambda = 1;

  @Parameter(names = {"-epsilon"}, description = "Epsilon value for HDRF strategy.")
  public double epsilon = 1;

  @Parameter(names = {"-partitions", "-p"}, description = "Number of partitions.", required = true)
  public int nPartitions = 1;

  @Parameter(names = {"-storage", "-s"}, description = "State storage type.", validateWith = StateStorageValidator.class, required = true)
  public String storage;

  @Parameter(names = {"-db"}, description = "Database URL or remote storage ip:port.")
  public String dbUrl;

  @Parameter(names = {"-user"}, description = "Database user.")
  public String user;

  @Parameter(names = {"-pass"}, description = "Database password.")
  public String pass = "";

  @Parameter(names = {"-reset"}, description = "Reset storage.", arity = 1)
  public boolean reset = true;

  @Parameter(names = {"-output"}, description = "Output file.")
  public String output = "";

  @Parameter(names = {"-d"}, description = "Delimiter. The default value is a space.")
  public String delimiter = " ";

  @Parameter(names = {"-append"}, description = "Append to the output file.", arity = 1)
  public boolean append = true;

  @Parameter(names = {"-shuffle"}, description = "Shuffle the input edges randomly.", arity = 1)
  public boolean shuffle = true;

  @Parameter(names = {"-sg"}, description = "Grouping edges based on their source ids.", arity = 1)
  public boolean srcGrouping = false;

  @Parameter(names = {"-pg"}, description = "Grouping edges based on their partitions after one iteration of streaming.", arity = 1)
  public boolean pGrouping = false;

  @Parameter(names = {"-ed"}, description = "Compute the exact degree of vertices before partitioning the graph.", arity = 1)
  public boolean exactDegree = false;

  @Parameter(names = {"-single"}, description = "Run a single thread experiment as a base for comparison.", arity = 1)
  public boolean single = true;

  //It's not implemented yet. It was implemented but removed.
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
        case REMOTE:
          break;
        default:
          throw new ParameterException(String.format("Partitioning method %s is not supported!", value));

      }
    }
  }
}
