package se.kth.scs.utils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Hooman
 */
public class PartitionerSettings {

  public String file;
  public String algorithm;
  public String delimiter;
  public int window;
  public int frequency;
  public double lambda;
  public double epsilon;
  public short k;
  public int tasks;
  public String storage;
  public String dbUrl;
  public String user;
  public String pass;
  public boolean reset;
  public String output;
  public boolean append;
  public List<Integer> delay;
  public int restream;
  public int numExperiments;
  public final int tb;
  public final int minT;
  public final int maxT;
  public final int wb;
  public final int minW;
  public final int maxW;
  public boolean shuffle;
  public boolean srcGrouping;
  public boolean single;
  public boolean pGrouping;
  public boolean exactDegree;

  public PartitionerSettings(int tb, int minT, int maxT, int wb, int minW, int maxW) {
    this.tb = tb;
    this.minT = minT;
    this.maxT = maxT;
    this.wb = wb;
    this.minW = minW;
    this.maxW = maxW;
  }

  public PartitionerSettings() {
    this(1, 1, 1, 1, 1, 1);
  }

  public void setSettings(PartitionerSettings settings) {
    file = settings.file;
    algorithm = settings.algorithm;
    delimiter = settings.delimiter;
    window = settings.window;
    frequency = settings.frequency;
    lambda = settings.lambda;
    epsilon = settings.lambda;
    k = settings.k;
    tasks = settings.tasks;
    storage = settings.storage;
    dbUrl = settings.dbUrl;
    user = settings.user;
    pass = settings.pass;
    reset = settings.reset;
    output = settings.output;
    append = settings.append;
    delay = settings.delay;
    restream = settings.restream;
    shuffle = settings.shuffle;
    srcGrouping = settings.srcGrouping;
    single = settings.single;
    exactDegree = settings.exactDegree;
    numExperiments = settings.numExperiments;
  }

  public void setSettings(PartitionerInputCommands commands) {
    k = (short) commands.nPartitions;
    file = commands.file;
    algorithm = commands.algorithm;
    output = commands.output;
    storage = commands.storage;
    dbUrl = commands.dbUrl;
    user = commands.user;
    pass = commands.pass;
    lambda = commands.lambda;
    epsilon = commands.epsilon;
    delay = commands.delay;
    if (delay == null || delay.size() != 2) {
      delay = new ArrayList<>(2);
      delay.add(0);
      delay.add(0);
    }
    append = commands.append;
    reset = commands.reset;
    delimiter = commands.delimiter;
    frequency = commands.partitionsUpdateFrequency;
    shuffle = commands.shuffle;
    srcGrouping = commands.srcGrouping;
    pGrouping = commands.pGrouping;
    single = commands.single;
    exactDegree = commands.exactDegree;
    numExperiments = commands.numExperiments;
  }

  /**
   * @return the file
   */
  public String getFile() {
    return file;
  }

  /**
   * @param file the file to set
   * @return
   */
  public PartitionerSettings setFile(String file) {
    this.file = file;
    return this;
  }

  /**
   * @return the window
   */
  public int getWindow() {
    return window;
  }

  /**
   * @param window the window to set
   * @return
   */
  public PartitionerSettings setWindow(int window) {
    this.window = window;
    return this;
  }

  /**
   * @return the lambda
   */
  public double getLambda() {
    return lambda;
  }

  /**
   * @param lambda the lambda to set
   * @return
   */
  public PartitionerSettings setLambda(double lambda) {
    this.lambda = lambda;
    return this;
  }

  /**
   * @return the epsilon
   */
  public double getEpsilon() {
    return epsilon;
  }

  /**
   * @param epsilon the epsilon to set
   * @return
   */
  public PartitionerSettings setEpsilon(double epsilon) {
    this.epsilon = epsilon;
    return this;
  }

  /**
   * @return the k
   */
  public int getK() {
    return k;
  }

  /**
   * @param k the k to set
   * @return
   */
  public PartitionerSettings setK(short k) {
    this.k = k;
    return this;
  }

  /**
   * @return the tasks
   */
  public int getTasks() {
    return tasks;
  }

  /**
   * @param tasks the tasks to set
   * @return
   */
  public PartitionerSettings setTasks(int tasks) {
    this.tasks = tasks;
    return this;
  }

  /**
   * @return the storage
   */
  public String getStorage() {
    return storage;
  }

  /**
   * @param storage the storage to set
   * @return
   */
  public PartitionerSettings setStorage(String storage) {
    this.storage = storage;
    return this;
  }

  /**
   * @return the dbUrl
   */
  public String getDbUrl() {
    return dbUrl;
  }

  /**
   * @param dbUrl the dbUrl to set
   * @return
   */
  public PartitionerSettings setDbUrl(String dbUrl) {
    this.dbUrl = dbUrl;
    return this;
  }

  /**
   * @return the user
   */
  public String getUser() {
    return user;
  }

  /**
   * @param user the user to set
   * @return
   */
  public PartitionerSettings setUser(String user) {
    this.user = user;
    return this;
  }

  /**
   * @return the pass
   */
  public String getPass() {
    return pass;
  }

  /**
   * @param pass the pass to set
   * @return
   */
  public PartitionerSettings setPass(String pass) {
    this.pass = pass;
    return this;
  }

  /**
   * @return the reset
   */
  public boolean isReset() {
    return reset;
  }

  /**
   * @param reset the reset to set
   * @return
   */
  public PartitionerSettings setReset(boolean reset) {
    this.reset = reset;
    return this;
  }

  /**
   * @return the output
   */
  public String getOutput() {
    return output;
  }

  /**
   * @param output the output to set
   * @return
   */
  public PartitionerSettings setOutput(String output) {
    this.output = output;
    return this;
  }

  /**
   * @return the append
   */
  public boolean isAppend() {
    return append;
  }

  /**
   * @param append the append to set
   * @return
   */
  public PartitionerSettings setAppend(boolean append) {
    this.append = append;
    return this;
  }

  /**
   * @return the delay
   */
  public List<Integer> getDelay() {
    return delay;
  }

  /**
   * @param delay the delay to set
   * @return
   */
  public PartitionerSettings setDelay(List<Integer> delay) {
    this.delay = delay;
    return this;
  }

  /**
   * @return the delimiter
   */
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * @param delimiter the delimiter to set
   */
  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

}
