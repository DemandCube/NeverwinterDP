package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.DataStreamSourceInterceptor;
import com.neverwinterdp.scribengin.dataflow.DataStreamType;
import com.neverwinterdp.scribengin.dataflow.MTService;
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
  
  private DataStreamType                dataStreamType = DataStreamType.Wire ;
  private boolean                       stopInput ;
  
  private MTService                     mtService ;
  
  public InputDataStreamContext(DataStreamOperatorRuntimeContext ctx, Storage storage, int partitionId) throws Exception {
    source = storage.getSource().getLatestSourcePartition();
    assignedPartition = source.getPartitionStream(partitionId);
    assignedPartitionReader = assignedPartition.getReader("DataflowTask");
    StorageConfig storageConfig = storage.getStorageConfig();
    
    if(storageConfig.booleanAttribute(DataSet.DATAFLOW_SOURCE_INPUT, false)) {
      dataStreamType = DataStreamType.Input;
    }
    
    String interceptorTypes = storageConfig.attribute(DataSet.DATAFLOW_SOURCE_INTERCEPTORS);
    interceptor = DataStreamSourceInterceptor.load(ctx, StringUtil.toStringArray(interceptorTypes));
    mtService = ctx.getService(MTService.class);
  }
  
  public DataStreamType getDataStreamType() { return dataStreamType; }
  
  public void stopInput() { stopInput = true; }
  
  public Message nextMessage(DataStreamOperatorRuntimeContext ctx, long maxWaitForDataRead) throws Exception {
    if(stopInput) return null;
    Message message = assignedPartitionReader.next(maxWaitForDataRead);
    if (message != null) {
      if(this.dataStreamType == DataStreamType.Input) {
        MessageTracking messageTracking = createMessageTracking(ctx);
        message.setMessageTracking(messageTracking);
        mtService.logInput(messageTracking);
      }
      
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
    if(dataStreamType == DataStreamType.Input) mtService.flushInput();
  }

  public void rollback() throws Exception {
    assignedPartitionReader.rollback();
  }

  public void close() throws Exception {
    assignedPartitionReader.close();
  }
  
  MessageTracking createMessageTracking(DataStreamOperatorContext ctx) throws Exception {
    MessageTracking mTracking = mtService.nextMessageTracking();
    String[] tag = { 
      "vm:" + ctx.getVM().getId(), "executor:" + ctx.getTaskExecutor().getId()
    };
    mTracking.add(new MessageTrackingLog("input", tag));
    return mTracking;
  }
}