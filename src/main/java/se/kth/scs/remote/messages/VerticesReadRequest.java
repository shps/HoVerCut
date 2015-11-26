package se.kth.scs.remote.messages;

import java.io.Serializable;

/**
 *
 * @author Hooman
 */
public class VerticesReadRequest implements Serializable {
  private static final long serialVersionUID = -8590121996866307930L;

  private final int[] vertices;

  public VerticesReadRequest(int[] vertices) {
    this.vertices = vertices;
  }

  /**
   * @return the vertices
   */
  public int[] getVertices() {
    return vertices;
  }

}
