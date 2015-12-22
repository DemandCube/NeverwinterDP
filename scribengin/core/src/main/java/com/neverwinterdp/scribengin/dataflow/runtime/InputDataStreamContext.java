package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.DataStreamSourceInterceptor;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.SourcePartition;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.util.text.StringUtil;

public class InputDataStreamContext {
  private SourcePartition               source;
  private SourcePartitionStream         assignedPartition;
  private SourcePartitionStreamReader   assignedPartitionReader;
  private DataStreamSourceInterceptor[] interceptor;

  public InputDataStreamContext(DataStreamOperatorRuntimeContext ctx, Storage storage, int partitionId) throws Exception {
    source = storage.getSource().getLatestSourcePartition();
    assignedPartition = source.getPartitionStream(partitionId);
    assignedPartitionReader = assignedPartition.getReader("DataflowTask");

    StorageConfig storageConfig = storage.getStorageConfig();
    String interceptorTypes = storageConfig.attribute(DataSet.DATAFLOW_SOURCE_INTERCEPTORS);
    interceptor = DataStreamSourceInterceptor.load(ctx, StringUtil.toStringArray(interceptorTypes));
  }
  
  public Message nextMessage(DataStreamOperatorRuntimeContext ctx, long maxWaitForDataRead) throws Exception {
    Message message = assignedPartitionReader.next(maxWaitForDataRead);
    if (message != null) {
      for (DataStreamSourceInterceptor sel : interceptor) {
        sel.onRead(ctx, message);
      }
    }
    return message;
  }
  
  public void prepareCommit() throws Exception {
    assignedPartitionReader.prepareCommit();
  }

  public void completeCommit() throws Exception {
    assignedPartitionReader.completeCommit();
  }

  public void rollback() throws Exception {
    assignedPartitionReader.rollback();
  }

  public void close() throws Exception {
    assignedPartitionReader.close();
  }
}