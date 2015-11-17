package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.Record;

/**
 * @author Tuan Nguyen
 */
public interface SourcePartitionStreamReader {
  public String getName() ;
  public Record next(long maxWait) throws Exception;
  public Record[] next(int size, long maxWait) throws Exception ;
  public boolean isEndOfDataStream() throws Exception ;
  public void rollback() throws Exception;
  public void prepareCommit() throws Exception ;
  public void completeCommit() throws Exception ;
  public void commit() throws Exception;
  public CommitPoint getLastCommitInfo() ;
  public void close() throws Exception;
}
