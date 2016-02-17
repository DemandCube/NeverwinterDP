package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.DataStreamSourceInterceptor;
import com.neverwinterdp.scribengin.dataflow.DataStreamType;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
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
    assignedPartitionReader = assignedPartition.getReader("DataflowInput-" + ctx.getDescriptor().getOperatorName());
    StorageConfig storageConfig = storage.getStorageConfig();
    
    if(storageConfig.booleanAttribute(DataSet.DATAFLOW_SOURCE_INPUT, false)) {
      dataStreamType = DataStreamType.Input;
      mtService = new MTService("input", ctx.getService(DataflowRegistry.class));
    }
    
    String interceptorTypes = storageConfig.attribute(DataSet.DATAFLOW_SOURCE_INTERCEPTORS);
    interceptor = DataStreamSourceInterceptor.load(ctx, StringUtil.toStringArray(interceptorTypes));
  }
  
  public DataStreamType getDataStreamType() { return dataStreamType; }
  
  public boolean isStopInput() { return stopInput; }
  
  public void stopInput() { stopInput = true; }
  
  public Message nextMessage(DataStreamOperatorRuntimeContext ctx, long maxWaitForDataRead) throws Exception {
    try {
    if(stopInput) return null;
    
    if(dataStreamType == DataStreamType.Input) {
      if(!mtService.hasNextMessageTracking(maxWaitForDataRead)) return null;
    }
    
    Message message = assignedPartitionReader.next(maxWaitForDataRead);
    if (message != null) {
      if(dataStreamType == DataStreamType.Input) {
        MessageTracking messageTracking = mtService.nextMessageTracking();
        String[] tag = { 
          "vm:" + ctx.getVM().getVmId(), "executor:" + ctx.getTaskExecutor().getId()
        };
        messageTracking.add(new MessageTrackingLog("input", tag));
        
        message.setMessageTracking(messageTracking);
      }
      
      for (DataStreamSourceInterceptor sel : interceptor) {
        sel.onRead(ctx, message);
      }
    }
    return message;
    } catch(Throwable t) {
      t.printStackTrace();
      return null;
    }
  }
  
  public void prepareCommit(DataStreamOperatorRuntimeContext ctx) throws Exception {
    assignedPartitionReader.prepareCommit();
    for (DataStreamSourceInterceptor sel : interceptor) {
      sel.onPrepareCommit(ctx);
    }
  }

  public void completeCommit(DataStreamOperatorRuntimeContext ctx) throws Exception {
    assignedPartitionReader.completeCommit();
    
    for (DataStreamSourceInterceptor sel : interceptor) {
      sel.onCompleteCommit(ctx);
    }
    
    if(dataStreamType == DataStreamType.Input) {
      mtService.flushWindows();
    }
  }

  public void rollback() throws Exception {
    assignedPartitionReader.rollback();
  }

  public void close() throws Exception {
    assignedPartitionReader.close();
  }
}