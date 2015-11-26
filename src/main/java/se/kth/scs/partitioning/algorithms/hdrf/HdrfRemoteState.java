/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.scs.partitioning.algorithms.hdrf;

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
import se.kth.scs.remote.messages.AllVerticesRequest;
import se.kth.scs.remote.messages.ClearAllRequest;
import se.kth.scs.remote.messages.CloseSessionRequest;
import se.kth.scs.remote.messages.PartitionsRequest;
import se.kth.scs.remote.messages.PartitionsResponse;
import se.kth.scs.remote.messages.PartitionsWriteRequest;
import se.kth.scs.remote.messages.VerticesReadRequest;
import se.kth.scs.remote.messages.VerticesReadResponse;
import se.kth.scs.remote.messages.VerticesWriteRequest;
import utils.FstStream;

/**
 *
 * @author Hooman
 */
public class HdrfRemoteState implements PartitionState {

  private final short k;
  private final String ip;
  private final int port;
  private final ThreadLocal<Socket> clients = new ThreadLocal<>();

  public HdrfRemoteState(short k, String ip, int port) throws IOException {
    this.k = k;
    this.ip = ip;
    this.port = port;
    Socket client = new Socket(ip, port);
    FstStream.writeObject(new ClearAllRequest(), client);
    FstStream.writeObject(new CloseSessionRequest(), client);
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
      Socket c = getClient();
      FstStream.writeObject(new CloseSessionRequest(), c);
      if (!c.isClosed()) {
        c.close();
      }
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
      Socket c = getClient();
      FstStream.writeObject(new AllVerticesRequest(), c);
      VerticesReadResponse reponse = (VerticesReadResponse) FstStream.readObject(c);
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
      Socket c = getClient();
      int[] ids = new int[vids.size()];
      int i = 0;
      for (int v : vids) {
        ids[i] = v;
        i++;
      }
      VerticesReadRequest request = new VerticesReadRequest(ids);
      FstStream.writeObject(request, c);
      VerticesReadResponse reponse = (VerticesReadResponse) FstStream.readObject(c);
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
      Socket c = getClient();
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
      FstStream.writeObject(request, c);
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
      PartitionsRequest request = new PartitionsRequest();
      FstStream.writeObject(request, c);
      PartitionsResponse response = (PartitionsResponse) FstStream.readObject(c);
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
      Socket c = getClient();
      int[] eDeltas = new int[ps.size()];
      for (Partition p : ps) {
        eDeltas[p.getId()] = p.getESizeDelta();
      }
      PartitionsWriteRequest request = new PartitionsWriteRequest(eDeltas);
      FstStream.writeObject(request, c);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private Socket getClient() throws IOException {
    Socket c = clients.get();
    if (c == null) {
      c = new Socket(ip, port);
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

}
