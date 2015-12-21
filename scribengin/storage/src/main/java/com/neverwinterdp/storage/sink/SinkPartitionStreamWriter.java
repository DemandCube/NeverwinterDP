package com.neverwinterdp.storage.sink;

import com.neverwinterdp.message.Message;

public interface SinkPartitionStreamWriter {
  /**
   * @param record
   * @return true if we should keep appending, false if ready to commit
   * @throws Exception
   */
  public void append(Message record) throws Exception ;
  public void commit() throws Exception ;
  public void close()  throws  Exception ;
  public void rollback() throws Exception;
  public void prepareCommit() throws Exception ;
  public void completeCommit() throws Exception ;
}
