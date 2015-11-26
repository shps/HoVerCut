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
public class VerticesWriteRequest implements Serializable {
  private static final long serialVersionUID = 3083427927860617772L;

  private final int[] vertices;
  private final int[] degreeDeltas;
  private final int[] partitionsDeltas;

  public VerticesWriteRequest(int[] vertices, int[] degreeDeltas, int[] partitionsDeltas) {
    this.vertices = vertices;
    this.degreeDeltas = degreeDeltas;
    this.partitionsDeltas = partitionsDeltas;
  }

  /**
   * @return the vertices
   */
  public int[] getVertices() {
    return vertices;
  }

  /**
   * @return the degreeDeltas
   */
  public int[] getDegreeDeltas() {
    return degreeDeltas;
  }

  /**
   * @return the partitionsDeltas
   */
  public int[] getPartitionsDeltas() {
    return partitionsDeltas;
  }

}
