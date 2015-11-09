package com.neverwinterdp.scribengin.storage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;

import com.neverwinterdp.util.JSONSerializer;

public class SegmentLock {
  private FileSystem             fs;
  private Path                   lockPath;
  private SegmentOperationConfig operationConfig;
  transient private boolean      owner = false;
  
  public SegmentLock(FileSystem fs, Path lockPath, SegmentOperationConfig operationConfig) {
    this.fs = fs ;
    this.lockPath = lockPath;
    this.operationConfig = operationConfig;
  }

  public Path getLockPath() {  return lockPath; }

  public SegmentOperationConfig getOperationConfig() { return operationConfig; }

  public boolean tryLock(long timeout, long tryPeriod) throws IOException, InterruptedException {
    long stopTime = System.currentTimeMillis() + timeout;
    while(stopTime > System.currentTimeMillis()) {
      boolean locked = lock() ;
      if(locked) return true;
      Thread.sleep(tryPeriod);
    }
    return false; 
  }
  
  synchronized public void update(SegmentOperationConfig config) throws IOException {
    checkOwner();
    FSDataOutputStream out = fs.create(lockPath, true);
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(config);
    out.write(bytes);
    out.hflush();
    out.close();
  }
  
  synchronized public boolean lock() throws IOException {
    if(owner) return true;
    try {
      if(fs.exists(lockPath)) return false ;
      FSDataOutputStream out = fs.create(lockPath, false);
      out.hsync();
      byte[] bytes = JSONSerializer.INSTANCE.toBytes(operationConfig);
      out.write(bytes);
      out.hflush();
      out.close();
      owner = true;
      return owner;
    } catch(AlreadyBeingCreatedException ex) {
      System.err.println("Lock Error: " + ex.getMessage());
      return false;
    } catch(IOException ex) {
      if(ex.getMessage().startsWith("File already exists")) {
        System.err.println("Lock Error: " + ex.getMessage());
        return false;
      }
      throw ex;
    }
  }
  
  synchronized public boolean unlock() throws IOException {
    checkOwner();
    boolean deleted = fs.delete(lockPath, false);
    owner = !deleted;
    //System.out.println("<== unlock, owner = " + owner + "\n");
    return deleted;
  }
  
  synchronized void discardDeathLock() throws IOException {
    if(fs.exists(lockPath)) {
    }
  }
  
  void checkOwner() throws IOException {
    if(!owner) {
      throw new IOException("Not the owner of the lock");
    }
  }
}