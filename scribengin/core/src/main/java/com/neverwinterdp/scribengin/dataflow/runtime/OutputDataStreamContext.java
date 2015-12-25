package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.DataStreamSinkInterceptor;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.text.StringUtil;

public class OutputDataStreamContext {
  private Sink                        sink;
  private SinkPartitionStream         assignedPartition;
  private SinkPartitionStreamWriter   assignedPartitionWriter;
  private DataStreamSinkInterceptor[] interceptor;

  public OutputDataStreamContext(DataStreamOperatorRuntimeContext ctx, Storage storage, int partitionId) throws Exception {
    sink = storage.getSink();
    assignedPartition = sink.getPartitionStream(partitionId);
    if(assignedPartition == null) {
      assignedPartition = sink.getPartitionStream(partitionId);
    }
    assignedPartitionWriter = assignedPartition.getWriter();
    
    StorageConfig storageConfig = storage.getStorageConfig();
    String interceptorTypes = storageConfig.attribute(DataSet.DATAFLOW_SINK_INTERCEPTORS);
    interceptor = DataStreamSinkInterceptor.load(ctx, StringUtil.toStringArray(interceptorTypes));
  }
  
  public void write(DataStreamOperatorRuntimeContext ctx, Message message) throws Exception {
    for(DataStreamSinkInterceptor sel : interceptor) {
      sel.onWrite(ctx, message);
    }
    assignedPartitionWriter.append(message);
  }

  public void prepareCommit() throws Exception {
    assignedPartitionWriter.prepareCommit();
  }

  public void completeCommit() throws Exception {
    assignedPartitionWriter.completeCommit();
  }

  public void rollback() throws Exception {
    assignedPartitionWriter.rollback();
  }

  public void close() throws Exception {
    assignedPartitionWriter.close();
  }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("Sink:\n").
      append("  Type = ").append(sink.getStorageConfig().getType()).
      append("  Stream Id = ").append(assignedPartition.getPartitionStreamId());
    return b.toString();
  }
}