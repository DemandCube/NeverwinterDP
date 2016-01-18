package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.DataStreamSinkInterceptor;
import com.neverwinterdp.scribengin.dataflow.DataStreamType;
import com.neverwinterdp.scribengin.dataflow.MTService;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
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
  private DataStreamType              dataStreamType = DataStreamType.Wire;
  private MTService                   mtService;

  public OutputDataStreamContext(DataStreamOperatorRuntimeContext ctx, Storage storage, int partitionId) throws Exception {
    sink = storage.getSink();
    assignedPartition = sink.getPartitionStream(partitionId);
    if(assignedPartition == null) {
      assignedPartition = sink.getPartitionStream(partitionId);
    }
    assignedPartitionWriter = assignedPartition.getWriter();
    
    StorageConfig storageConfig = storage.getStorageConfig();
    
    if(storageConfig.booleanAttribute(DataSet.DATAFLOW_SINK_OUTPUT, false)) {
      dataStreamType = DataStreamType.Output;
      mtService = new MTService("output", ctx.getService(DataflowRegistry.class));
    }
    
    String interceptorTypes = storageConfig.attribute(DataSet.DATAFLOW_SINK_INTERCEPTORS);
    interceptor = DataStreamSinkInterceptor.load(ctx, StringUtil.toStringArray(interceptorTypes));
  }
  
  public void write(DataStreamOperatorRuntimeContext ctx, Message message) throws Exception {
    for(DataStreamSinkInterceptor sel : interceptor) sel.onWrite(ctx, message);
    assignedPartitionWriter.append(message);

    if(dataStreamType == DataStreamType.Output) {
      MessageTracking messageTracking = message.getMessageTracking();
      String[] tag = { 
          "vm:" + ctx.getVM().getVmId(), "executor:" + ctx.getTaskExecutor().getId()
      };
      messageTracking.add(new MessageTrackingLog("output", tag));
      mtService.log(messageTracking);
    }
  }

  public void prepareCommit(DataStreamOperatorRuntimeContext ctx) throws Exception {
    assignedPartitionWriter.prepareCommit();
    for(DataStreamSinkInterceptor sel : interceptor) {
      sel.onPrepareCommit(ctx);
    }
  }

  public void completeCommit(DataStreamOperatorRuntimeContext ctx) throws Exception {
    assignedPartitionWriter.completeCommit();
    for(DataStreamSinkInterceptor sel : interceptor) {
      sel.onCompleteCommit(ctx);
    }
    if(dataStreamType == DataStreamType.Output) {
      mtService.flushWindowStats();
    }
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