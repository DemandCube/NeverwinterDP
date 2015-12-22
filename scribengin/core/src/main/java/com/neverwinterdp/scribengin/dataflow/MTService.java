package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.registry.RegistryException;

public interface MTService {
  public MessageTracking nextMessageTracking() throws RegistryException ;
  public void log(MessageTracking messageTracking) throws RegistryException ;
}
