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
public class VerticesReadResponse implements Serializable {

  private final long[] vertices;
  private final int[] degrees;
  private final byte[] partitions;

  public VerticesReadResponse(long[] vertices, int[] degrees, byte[] partitions) {
    this.vertices = vertices;
    this.degrees = degrees;
    this.partitions = partitions;
  }

  /**
   * @return the vertices
   */
  public long[] getVertices() {
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
  public byte[] getPartitions() {
    return partitions;
  }
}
