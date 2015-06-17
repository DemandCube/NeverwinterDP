package com.neverwinterdp.vm.client;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.vm.VMConfig;

public class GroupVMSubmitter {
  private VMClient vmClient ;
  private List<VMSubmitter> vmSubmitters = new ArrayList<>() ;
  
  public GroupVMSubmitter(VMClient vmClient) {
    this.vmClient = vmClient ;
  }
  
  public void add(String appHome, VMConfig vmConfig) {
    VMSubmitter vmSubmitter = new VMSubmitter(vmClient, appHome, vmConfig);
    vmSubmitters.add(vmSubmitter);
  }
  
  public void submit() throws Exception {
    for(VMSubmitter sel : vmSubmitters) {
      sel.submit();
    }
  }
  
  public void submitAndWaitForRunning(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    for(VMSubmitter sel : vmSubmitters) {
      sel.submit();
      long waitTime = stopTime - System.currentTimeMillis();
      if(waitTime <= 0) {
        throw new Exception("Cannot wait for all vm running in " + timeout + "ms") ;
      }
      sel.waitForRunning(waitTime);
    }
  }
  
  public void waitForRunning(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    for(VMSubmitter sel : vmSubmitters) {
      long waitTime = stopTime - System.currentTimeMillis();
      if(waitTime <= 0) {
        throw new Exception("Cannot wait for all vm running in " + timeout + "ms") ;
      }
      sel.waitForRunning(waitTime);
    }
  }
  
  public void waitForTerminated(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    for(VMSubmitter sel : vmSubmitters) {
      long waitTime = stopTime - System.currentTimeMillis();
      if(waitTime <= 0) {
        throw new Exception("Cannot wait for all vm teminated in " + timeout + "ms") ;
      }
      sel.waitForTerminated(waitTime);
    }
  }
}