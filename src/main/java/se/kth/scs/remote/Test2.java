/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.scs.remote;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import se.kth.scs.remote.messages.ClearAllRequest;

/**
 *
 * @author Hooman
 */
public class Test2 {

  static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

  public static void main(String[] args) throws IOException {
    Socket c = new Socket("localhost", 4444);

    for (int i = 0; i < 10; i++) {
      DataOutputStream output = new DataOutputStream(c.getOutputStream());
      DataInputStream input = new DataInputStream(c.getInputStream());
      byte[] barray = conf.asByteArray(new ClearAllRequest());
      output.writeInt(barray.length);
      output.write(barray);
      output.flush();
    }
  }

}
