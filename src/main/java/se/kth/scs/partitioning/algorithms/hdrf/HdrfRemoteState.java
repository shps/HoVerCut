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
import org.apache.commons.lang.ArrayUtils;
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

  private final int k;
  private final String ip;
  private final int port;
  private final ThreadLocal<ClientSocket> clients = new ThreadLocal<>();

  public HdrfRemoteState(int k, String ip, int port) throws IOException {
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
  public int getNumberOfPartitions() {
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
  public Vertex getVertex(long vid) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Map<Long, Vertex> getAllVertices() {
    Map<Long, Vertex> vertices = null;
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

  private Map<Long, Vertex> deserializeVertices(VerticesReadResponse response) {
    Map<Long, Vertex> vertices = new HashMap<>();
    long[] vIds = response.getVertices();
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
  public Map<Long, Vertex> getVertices(Set<Long> vids) {
    Map<Long, Vertex> vertices = null;
    try {
      ClientSocket c = getClient();
      long[] ids = new long[vids.size()];
      int i = 0;
      for (Long v : vids) {
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
      long[] vids = new long[vs.size()];
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
  public Partition getPartition(int pid) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public List<Partition> getPartions(int[] pids) {
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
      int[] vDeltas = new int[ps.size()];
      for (Partition p : ps) {
        eDeltas[p.getId()] = p.getESizeDelta();
        vDeltas[p.getId()] = p.getVSizeDelta();
      }
      PartitionsWriteRequest request = new PartitionsWriteRequest(eDeltas, vDeltas);
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
    int[] vSizes = response.getVSizes();
    List<Partition> partitions = new ArrayList<>(eSizes.length);
    for (int i = 0; i < eSizes.length; i++) {
      Partition p = new Partition(i);
      p.setESize(eSizes[i]);
      p.setVSize(vSizes[i]);
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
