package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;

/**
 * @author Tuan Nguyen
 */
public interface SourceStreamReader {
  public String getName() ;
  public DataflowMessage next(long maxWait) throws Exception;
  public DataflowMessage[] next(int size, long maxWait) throws Exception ;
  public boolean isEndOfDataStream() throws Exception ;
  public void rollback() throws Exception;
  public void prepareCommit() throws Exception ;
  public void completeCommit() throws Exception ;
  public void commit() throws Exception;
  public CommitPoint getLastCommitInfo() ;
  public void close() throws Exception;
}
