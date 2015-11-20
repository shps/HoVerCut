/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.scs.partitioning.algorithms.hdrf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import se.kth.scs.remote.messages.AllVerticesRequest;
import se.kth.scs.remote.messages.ClearAllRequest;
import se.kth.scs.remote.messages.CloseSessionRequest;
import se.kth.scs.remote.messages.PartitionsRequest;
import se.kth.scs.remote.messages.PartitionsResponse;
import se.kth.scs.remote.messages.PartitionsWriteRequest;
import se.kth.scs.remote.messages.VerticesReadRequest;
import se.kth.scs.remote.messages.VerticesReadResponse;
import se.kth.scs.remote.messages.VerticesWriteRequest;

/**
 *
 * @author Hooman
 */
public class HdrfRemoteState implements PartitionState {

  private final short k;
  private final String ip;
  private final int port;
  private final ThreadLocal<ClientSocket> clients = new ThreadLocal<>();

  public HdrfRemoteState(short k, String ip, int port) throws IOException {
    this.k = k;
    this.ip = ip;
    this.port = port;
    ClientSocket client = new ClientSocket(new Socket(ip, port));
    client.getOutput().writeObject(new ClearAllRequest());
    client.getOutput().flush();
    client.getOutput().writeObject(new CloseSessionRequest());
    client.getOutput().flush();
    client.close();
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
  public void releaseResources() {
    //
  }

  @Override
  public void releaseTaskResources() {
    try {
      ClientSocket c = getClient();
      c.getOutput().writeObject(new CloseSessionRequest());
      c.getOutput().flush();
      c.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public Vertex getVertex(int vid) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Map<Integer, Vertex> getAllVertices() {
    Map<Integer, Vertex> vertices = null;
    try {
      ClientSocket c = getClient();
      c.getOutput().writeObject(new AllVerticesRequest());
      c.getOutput().flush();
      VerticesReadResponse reponse = (VerticesReadResponse) c.getInput().readObject();
      vertices = deserializeVertices(reponse);
    } catch (IOException | ClassNotFoundException ex) {
      ex.printStackTrace();
    }
    return vertices;
  }

  private Map<Integer, Vertex> deserializeVertices(VerticesReadResponse response) {
    Map<Integer, Vertex> vertices = new HashMap<>();
    int[] vIds = response.getVertices();
    int[] degrees = response.getDegrees();
    int[] partitions = response.getPartitions();
    for (int i = 0; i < vIds.length; i++) {
      Vertex v = new Vertex(vIds[i]);
      v.setpDegree(degrees[i]);
      v.setPartitions(partitions[i]);
      vertices.put(v.getId(), v);
    }
    return vertices;
  }

  @Override
  public Map<Integer, Vertex> getVertices(Set<Integer> vids) {
    Map<Integer, Vertex> vertices = null;
    try {
      ClientSocket c = getClient();
      int[] ids = new int[vids.size()];
      int i = 0;
      for (int v : vids) {
        ids[i] = v;
        i++;
      }
      VerticesReadRequest request = new VerticesReadRequest(ids);
      c.getOutput().writeObject(request);
      c.getOutput().flush();
      VerticesReadResponse reponse = (VerticesReadResponse) c.getInput().readObject();
      vertices = deserializeVertices(reponse);
    } catch (IOException | ClassNotFoundException ex) {
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
      ClientSocket c = getClient();
      int[] vids = new int[vs.size()];
      int[] degrees = new int[vs.size()];
      int[] ps = new int[vs.size()];
      int i = 0;
      for (Vertex v : vs) {
        vids[i] = v.getId();
        degrees[i] = v.getDegreeDelta();
        ps[i] = v.getPartitionsDelta();
        i++;
      }
      VerticesWriteRequest request = new VerticesWriteRequest(vids, degrees, ps);
      c.getOutput().writeObject(request);
      c.getOutput().flush();
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
      ClientSocket c = getClient();
      PartitionsRequest request = new PartitionsRequest();
      c.getOutput().writeObject(request);
      c.getOutput().flush();
      PartitionsResponse response = (PartitionsResponse) c.getInput().readObject();
      partitions = deserializePartititions(response);
    } catch (IOException | ClassNotFoundException ex) {
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
      ClientSocket c = getClient();
      int[] eDeltas = new int[ps.size()];
      for (Partition p : ps) {
        eDeltas[p.getId()] = p.getESizeDelta();
      }
      PartitionsWriteRequest request = new PartitionsWriteRequest(eDeltas);
      c.getOutput().writeObject(request);
      c.getOutput().flush();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private ClientSocket getClient() throws IOException {
    ClientSocket c = clients.get();
    if (c == null) {
      c = new ClientSocket(new Socket(ip, port));
      clients.set(c);
    }

    return c;
  }

  private List<Partition> deserializePartititions(PartitionsResponse response) {
    int[] eSizes = response.getESizes();
    List<Partition> partitions = new ArrayList<>(eSizes.length);
    for (short i = 0; i < eSizes.length; i++) {
      Partition p = new Partition(i);
      p.setESize(eSizes[i]);
      partitions.add(p);
    }

    return partitions;
  }

  private class ClientSocket {

    private final Socket socket;
    private final ObjectInputStream input;
    private final ObjectOutputStream output;

    public ClientSocket(Socket socket) throws IOException {
      this.socket = socket;
      output = new ObjectOutputStream(socket.getOutputStream());
      input = new ObjectInputStream(socket.getInputStream());
    }

    /**
     * @return the socket
     */
    public Socket getSocket() {
      return socket;
    }

    /**
     * @return the input
     */
    public ObjectInputStream getInput() {
      return input;
    }

    /**
     * @return the output
     */
    public ObjectOutputStream getOutput() {
      return output;
    }

    private void close() throws IOException {
      input.close();
      output.close();
      socket.close();
    }
  }

}
