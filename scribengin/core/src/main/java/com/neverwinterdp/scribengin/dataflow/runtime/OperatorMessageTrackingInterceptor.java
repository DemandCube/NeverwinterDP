package com.neverwinterdp.scribengin.dataflow.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.MessageTracking;
import com.neverwinterdp.message.MessageTrackingLogChunk;
import com.neverwinterdp.message.MessageTrackingLog;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorInterceptor;

public class OperatorMessageTrackingInterceptor implements DataStreamOperatorInterceptor {
  final static public int MAX_MESSAGE_PER_CHUNK = 4 * 1024;
  
  private int                  currentChunkId = 0;
  private AtomicInteger        idTracker = new AtomicInteger();
  private MessageTrackingLogChunk messageTrackingChunk;
  
  @Override
  public void preProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
  }

  @Override
  public void postProcess(DataStreamOperatorContext ctx, Message message) throws Exception {
    MessageTracking mTracking = message.getMessageTracking();
    if(mTracking == null) {
      mTracking = createMessageTracking();
      message.setMessageTracking(mTracking);
    }
    DataStreamOperatorDescriptor descriptor = ctx.getDescriptor();
    long timestamp = System.currentTimeMillis() ;
    MessageTrackingLog log = new MessageTrackingLog(descriptor.getOperatorName(), timestamp, (short) 0);
    String[] tag = { 
      "vm:" + ctx.getVM().getId(), "executor:" + ctx.getTaskExecutor().getId()
    };
    log.setTags(tag);
    mTracking.add(log);
    if(messageTrackingChunk == null) {
      int chunkId = 0;
      messageTrackingChunk = new MessageTrackingLogChunk("", chunkId, MAX_MESSAGE_PER_CHUNK);
    }
    messageTrackingChunk.log(mTracking, log);
  }
  
  private MessageTracking createMessageTracking() throws Exception {
    return new MessageTracking(currentChunkId, idTracker.getAndIncrement());
  }
}
