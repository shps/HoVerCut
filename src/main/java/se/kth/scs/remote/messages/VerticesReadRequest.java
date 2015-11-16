package se.kth.scs.remote.messages;

import java.io.Serializable;

/**
 *
 * @author Hooman
 */
public class VerticesReadRequest implements Serializable {

  private final long[] vertices;

  public VerticesReadRequest(long[] vertices) {
    this.vertices = vertices;
  }

  /**
   * @return the vertices
   */
  public long[] getVertices() {
    return vertices;
  }

}
