package se.kth.scs.partitioning.hovercut;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;
import se.kth.scs.remote.messages.Protocol;
import se.kth.scs.remote.messages.Serializer;

/**
 * A thread-safe implementation of a client to access the remote state storage.
 *
 * @author Hooman
 */
public class HovercutRemoteState implements PartitionState {

  private final short k;
  private final String ip;
  private final int port;
  private final ThreadLocal<Socket> clients = new ThreadLocal<>();

  public HovercutRemoteState(short k, String ip, int port, boolean exactDegree) throws IOException {
    this.k = k;
    this.ip = ip;
    this.port = port;
    try (Socket client = new Socket(ip, port)) {
      DataOutputStream output = new DataOutputStream(client.getOutputStream());
      if (!exactDegree) {
        output.writeByte(Protocol.CLEAR_ALL_REQUEST);
      } else {
        output.writeByte(Protocol.CLEAR_ALL_BUT_DEGREE_REQUEST);
      }
      output.flush();
      output.writeByte(Protocol.CLOSE_SESSION_REQUEST);
      output.flush();
    }
  }

  @Override
  public short getNumberOfPartitions() {
    return k;
  }

  @Override
  public void applyState() {
    //
  }

  @Override
  public void releaseResources(boolean clearAll) {
    //
  }

  @Override
  public void releaseTaskResources() {
    try {
      Socket c = getClient();
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      output.writeByte(Protocol.CLOSE_SESSION_REQUEST);
      output.flush();
      if (!c.isClosed()) {
        c.close();
        c = null;
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public Vertex getVertex(int vid) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  private Map<Integer, Vertex> deserializeVertices(int[] r) {
    Map<Integer, Vertex> vertices = new HashMap<>();
    for (int i = 0; i < r.length; i = i + 3) {
      Vertex v = new Vertex(r[i]);
      v.setpDegree(r[i + 1]);
      v.setPartitions(r[i + 2]);
      vertices.put(v.getId(), v);
    }
    return vertices;
  }

  @Override
  public Map<Integer, Vertex> getVertices(Set<Integer> vids) {
    Map<Integer, Vertex> vertices = null;
    try {
      Socket c = getClient();
      DataInputStream input = new DataInputStream(c.getInputStream());
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      int[] ids = new int[vids.size()];
      int i = 0;
      for (int v : vids) {
        ids[i] = v;
        i++;
      }
      Serializer.serializeRequest(output, Protocol.VERTICES_READ_REQUEST, ids);
      int[] response = Serializer.deserializeRequest(input);
      vertices = deserializeVertices(response);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return vertices;
  }

  @Override
  public void putVertex(Vertex v) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void putVertices(Collection<Vertex> vs) {
    try {
      Socket c = getClient();
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      int[] vertices = new int[vs.size() * 3];
      int i = 0;
      for (Vertex v : vs) {
        vertices[i] = v.getId();
        vertices[i + 1] = v.getDegreeDelta();
        vertices[i + 2] = v.getPartitionsDelta();
        i = i + 3;
      }
      Serializer.serializeRequest(output, Protocol.VERTICES_WRITE_REQUEST, vertices);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public Partition getPartition(short pid) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public List<Partition> getPartions(short[] pids) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public List<Partition> getAllPartitions() {
    List<Partition> partitions = null;
    try {
      Socket c = getClient();
      DataInputStream input = new DataInputStream(c.getInputStream());
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      output.writeByte(Protocol.PARTITIONS_REQUEST);
      output.flush();
      int[] ps = Serializer.deserializeRequest(input);
      partitions = deserializePartititions(ps);
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return partitions;
  }

  @Override
  public void putPartition(Partition p) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void putPartitions(List<Partition> ps) {
    try {
      Socket c = getClient();
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      int[] eDeltas = new int[ps.size()];
      for (Partition p : ps) {
        eDeltas[p.getId()] = p.getESizeDelta();
      }
      Serializer.serializeRequest(output, Protocol.PARTITIONS_WRITE_REQUEST, eDeltas);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private Socket getClient() throws IOException {
    Socket c = clients.get();
    if (c == null || c.isClosed()) {
      c = new Socket(ip, port);
      clients.set(c);
    }

    return c;
  }

  private List<Partition> deserializePartititions(int[] r) {
    List<Partition> partitions = new ArrayList<>(r.length);
    for (short i = 0; i < r.length; i++) {
      Partition p = new Partition(i);
      p.setESize(r[i]);
      partitions.add(p);
    }

    return partitions;
  }

  @Override
  public Map<Integer, Vertex> getAllVertices(int expectedSize) {
    Map<Integer, Vertex> vertices = null;
    try {
      Socket c = getClient();
      DataInputStream input = new DataInputStream(c.getInputStream());
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      output.writeByte(Protocol.ALL_VERTICES_REQUEST);
      output.writeInt(expectedSize);
      output.flush();
      int[] response = Serializer.deserializeRequest(input);
      vertices = deserializeVertices(response);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return vertices;
  }

  @Override
  public void waitForAllUpdates(int expectedSize) {
    try {
      Socket c = getClient();
      DataInputStream input = new DataInputStream(c.getInputStream());
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      output.writeByte(Protocol.WAIT_FOR_ALL_UPDATES_REQUEST);
      output.writeInt(expectedSize);
      output.flush();
      byte response = input.readByte();
      if (response != Protocol.WAIT_FOR_ALL_UPDATES_RESPONSE) {
        throw new Exception("wrong response from the storage server!");
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

}
