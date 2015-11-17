/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 *
 * @author Hooman
 */
public class StorageInputCommands {

  @Parameter(names = {"-partitions", "-p"}, description = "Number of partitions.", required = true)
  public int nPartitions = 1;

  @Parameter(names = {"-a"}, description = "Storage ip:port.", validateWith = AddressValidator.class, required = true)
  public String address;

  public static class AddressValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      String[] addr = value.split(":");
      if (addr.length != 2) {
        throw new ParameterException(String.format("Address should be in the format of host:port!", value));
      }
    }
  }
}
