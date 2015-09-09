package com.neverwinterdp.scribengin.storage.hdfs.segment;

import java.util.ArrayList;
import java.util.List;

public class BatchOperation {
  private long startTime ;
  private long expireTime ;
  private List<OperationConfig> operations = new ArrayList<>();
  
  public BatchOperation() {}
  
  public BatchOperation(long expireTime) {
    this.startTime  = System.currentTimeMillis();
    this.expireTime = expireTime;
  }
  
  public long getStartTime() { return startTime; }
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
  
  public long getExpireTime() { return expireTime; }
  public void setExpireTime(long expireTime) {
    this.expireTime = expireTime;
  }
  
  public List<OperationConfig> getOperations() { return operations; }
  public void setOperations(List<OperationConfig> operations) { this.operations = operations; }
  
  public BatchOperation add(OperationConfig config) {
    operations.add(config);
    return this;
  }
  
  public void execute(SegmentStorage storage) throws Exception {
    for(int i = 0; i < operations.size(); i++) {
      OperationConfig config = operations.get(i);
      Class<? extends Operation> opClass = 
        (Class<? extends Operation>) Class.forName(config.getOperationClass()) ;
      Operation op = opClass.newInstance();
      op.execute(storage, config);
    }
  }
}