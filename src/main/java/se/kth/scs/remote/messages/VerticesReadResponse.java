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
public class VerticesReadResponse implements Serializable {
  private static final long serialVersionUID = -509337766467432309L;

  private final int[] vertices;
  private final int[] degrees;
  private final int[] partitions;

  public VerticesReadResponse(int[] vertices, int[] degrees, int[] partitions) {
    this.vertices = vertices;
    this.degrees = degrees;
    this.partitions = partitions;
  }

  /**
   * @return the vertices
   */
  public int[] getVertices() {
    return vertices;
  }

  /**
   * @return the degrees
   */
  public int[] getDegrees() {
    return degrees;
  }

  /**
   * @return the partitions
   */
  public int[] getPartitions() {
    return partitions;
  }
}
