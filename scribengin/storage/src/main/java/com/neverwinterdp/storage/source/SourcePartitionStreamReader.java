package com.neverwinterdp.storage.source;

import com.neverwinterdp.message.Message;

/**
 * @author Tuan Nguyen
 */
public interface SourcePartitionStreamReader {
  public String getName() ;
  public Message next(long maxWait) throws Exception ;
  public Message[] next(int size, long maxWait) throws Exception ;
  public boolean isEndOfDataStream() throws Exception ;
  public void rollback() throws Exception;
  public void prepareCommit() throws Exception ;
  public void completeCommit() throws Exception ;
  public void commit() throws Exception;
  public CommitPoint getLastCommitInfo() ;
  public void close() throws Exception;
}
