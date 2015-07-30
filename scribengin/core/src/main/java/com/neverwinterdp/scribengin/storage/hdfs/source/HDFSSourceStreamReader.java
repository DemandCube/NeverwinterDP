package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourceStreamReader implements SourceStreamReader {
  private String name;
  private FileSystem fs;
  private List<Path> dataPaths = new ArrayList<Path>();
  private int currentDataPathPos = -1;
  private FSDataInputStream currentDataPathInputStream;
  private boolean endOfStream = false; 

  private int commitPoint;
  private int currPosition;
  private CommitPoint lastCommitInfo;

  public HDFSSourceStreamReader(String name, FileSystem fs, StreamDescriptor descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.name = name;
    this.fs = fs;
    FileStatus[] status = fs.listStatus(new Path(descriptor.getLocation()));
    for (int i = 0; i < status.length; i++) {
      String path = status[i].getPath().getName();
      if(path.startsWith("data-") && path.endsWith(".dat")) {
        dataPaths.add(status[i].getPath());
      }
    }
  }

  public String getName() { return name; }

  public DataflowMessage next(long maxWait) throws Exception {
    if(currentDataPathInputStream == null) {
      currentDataPathInputStream = nextDataPathInputStream();
      if (currentDataPathInputStream == null) return null;
    }
    
    if(currentDataPathInputStream.available() <= 0) {
      currentDataPathInputStream.close();
      currentDataPathInputStream = nextDataPathInputStream();
      if (currentDataPathInputStream == null) return null;
    }
    
    int recordSize = currentDataPathInputStream.readInt();
    byte[] data = new byte[recordSize];
    currentDataPathInputStream.readFully(data);
    return JSONSerializer.INSTANCE.fromBytes(data, DataflowMessage.class);
  }

  public DataflowMessage[] next(int size, long maxWait) throws Exception {
    List<DataflowMessage> holder = new ArrayList<DataflowMessage>();
    DataflowMessage[] array = new DataflowMessage[holder.size()];
    for (int i = 0; i < size; i++) {
      DataflowMessage dataflowMessage = next(maxWait);
      if (dataflowMessage != null)
        holder.add(dataflowMessage);
      else
        break;
    }
    holder.toArray(array);
    return array;
  }

  public boolean isEndOfDataStream() { return endOfStream; }
  
  public void rollback() throws Exception {
    System.err.println("This method is not implemented");
    currPosition = commitPoint;
  }

  @Override
  public void prepareCommit() {
  }

  @Override
  public void completeCommit() {
    // TODO Auto-generated method stub
  }

  public void commit() throws Exception {
    System.err.println("This method is not implemented");
    lastCommitInfo = new CommitPoint(name, commitPoint, currPosition);
    this.commitPoint = currPosition;
  }

  public CommitPoint getLastCommitInfo() {
    return this.lastCommitInfo;
  }

  public void close() throws Exception {
  }

  private FSDataInputStream nextDataPathInputStream() throws IOException {
    currentDataPathPos++;
    if (currentDataPathPos >= dataPaths.size()) {
      endOfStream = true;
      return null;
    }
    FSDataInputStream is = fs.open(dataPaths.get(currentDataPathPos));
    if(is.available() <= 0) {
      is.close();
      return nextDataPathInputStream();
    }
    return is;
  }
}