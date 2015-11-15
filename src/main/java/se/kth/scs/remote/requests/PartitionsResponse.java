/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.scs.remote.requests;

import java.io.Serializable;

/**
 *
 * @author Hooman
 */
public class PartitionsResponse implements Serializable {

  private final int[] eSizes;
  private final int[] vSizes;

  public PartitionsResponse(int[] eSizes, int[] vSizes) {
    this.eSizes = eSizes;
    this.vSizes = vSizes;
  }

  /**
   * @return the eSizes
   */
  public int[] getESizes() {
    return eSizes;
  }

  /**
   * @return the vSizes
   */
  public int[] getVSizes() {
    return vSizes;
  }
}
