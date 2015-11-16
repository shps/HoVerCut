/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.scs.remote.messages;

import java.io.Serializable;

/**
 *
 * @author Hooman
 */
public class PartitionsWriteRequest implements Serializable {

  private final int[] eDeltas;
  private final int[] vDeltas;

  public PartitionsWriteRequest(int[] eDeltas, int[] vDeltas) {
    this.eDeltas = eDeltas;
    this.vDeltas = vDeltas;
  }

  /**
   * @return the eDeltas
   */
  public int[] geteDeltas() {
    return eDeltas;
  }

  /**
   * @return the vDeltas
   */
  public int[] getvDeltas() {
    return vDeltas;
  }
}
