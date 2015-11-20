
package se.kth.scs.remote.messages;

import java.io.Serializable;

/**
 *
 * @author Hooman
 */
public class PartitionsResponse implements Serializable {

  private final int[] eSizes;

  public PartitionsResponse(int[] eSizes) {
    this.eSizes = eSizes;
  }

  /**
   * @return the eSizes
   */
  public int[] getESizes() {
    return eSizes;
  }
}
