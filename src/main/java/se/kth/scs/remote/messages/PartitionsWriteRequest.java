package se.kth.scs.remote.messages;

import java.io.Serializable;

/**
 *
 * @author Hooman
 */
public class PartitionsWriteRequest implements Serializable {
  private static final long serialVersionUID = -2309871953447025368L;

  private final int[] eDeltas;

  public PartitionsWriteRequest(int[] eDeltas) {
    this.eDeltas = eDeltas;
  }

  /**
   * @return the eDeltas
   */
  public int[] geteDeltas() {
    return eDeltas;
  }
}
