package com.neverwinterdp.scribengin.dataflow.runtime.master;

public class DataflowMasterRuntimeReport {
  private String  vmId ;
  private boolean leader;
  
  public String getVmId() { return vmId; }
  public void setVmId(String vmId) { this.vmId = vmId; }
  
  public boolean isLeader() { return leader; }
  public void setLeader(boolean leader) { this.leader = leader; }
}
