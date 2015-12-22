package com.neverwinterdp.scribengin.dataflow.runtime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.task.TaskExecutorDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.WorkerService;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.StorageService;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.Meter;

public class DataStreamOperatorRuntimeContext implements DataStreamOperatorContext {
  private WorkerService                workerService;
  private TaskExecutorDescriptor       taskExecutor;
  private DataStreamOperatorDescriptor descriptor;
  private DataStreamOperatorReport     report;

  private InputDataStreamContext               inputContext;
  private Map<String, OutputDataStreamContext> outputContexts = new HashMap<>();
  
  private boolean complete = false;
  private Meter   dataflowReadMeter;
  private Meter   dataflowRecordMeter;

  public DataStreamOperatorRuntimeContext(WorkerService workerService, TaskExecutorDescriptor taskExecutor, DataStreamOperatorDescriptor taskConfig, DataStreamOperatorReport report) throws Exception {
    this.workerService = workerService;
    this.taskExecutor  = taskExecutor;
    this.descriptor    = taskConfig;
    this.report        = report;
    
    DataflowRegistry dflRegistry = workerService.getDataflowRegistry();
    StorageService storageService = workerService.getStorageService();
    StorageConfig inputConfig = dflRegistry.getStreamRegistry().getStream(taskConfig.getInput()) ;
    Storage inputStorage = storageService.getStorage(inputConfig);
    int partitionId = taskConfig.getInputPartitionId();
    inputContext = new InputDataStreamContext(this, inputStorage, partitionId);
    for(String output : taskConfig.getOutputs()) {
      StorageConfig outputConfig = dflRegistry.getStreamRegistry().getStream(output) ;
      Storage outputStorage = storageService.getStorage(outputConfig);
      OutputDataStreamContext outputContext = new OutputDataStreamContext(this, outputStorage, partitionId);
      outputContexts.put(output, outputContext);
    }

    dataflowReadMeter   = 
        workerService.getMetricRegistry().getMeter("dataflow.source." + taskConfig.getInput() + ".throughput.byte", "byte") ;
    dataflowRecordMeter = 
        workerService.getMetricRegistry().getMeter("dataflow.source." + taskConfig.getInput() + ".throughput.record", "record") ;
  }
  
  public DataStreamOperatorDescriptor getDescriptor() { return this.descriptor; }
  
  public DataStreamOperatorReport getReport() { return this.report; }

  public TaskExecutorDescriptor getTaskExecutor() { return taskExecutor; }
  
  public VMDescriptor getVM() { return workerService.getVMDescriptor(); }
  
  public <T> T getService(Class<T> type) {
    return workerService.getServiceContainer().getInstance(type);
  }
  
  public boolean isComplete() { return this.complete ; }
  
  public void setComplete() { this.complete = true; }

  public Set<String> getAvailableOutputs() { return descriptor.getOutputs(); }
  
  public Message nextMessage(long maxWaitForDataRead) throws Exception {
    Message dataflowMessage = inputContext.nextMessage(this, maxWaitForDataRead);
    if(dataflowMessage != null) {
      dataflowReadMeter.mark(dataflowMessage.getData().length + dataflowMessage.getKey().length());
      dataflowRecordMeter.mark(1);
    }
    return dataflowMessage ;
  }
  
  public void write(String name, Message message) throws Exception {
    OutputDataStreamContext sinkContext = outputContexts.get(name);
    sinkContext.write(this, message);
    
    Meter meter = 
        workerService.getMetricRegistry().getMeter("dataflow.sink." + name + ".throughput.byte", "byte") ;
    meter.mark(message.getData().length + message.getKey().length());
    Meter recordMetter = 
        workerService.getMetricRegistry().getMeter("dataflow.sink." + name + ".throughput.record", "record") ;
    recordMetter.mark(1);
  }
  
  private void prepareCommit() throws Exception {
    Iterator<OutputDataStreamContext> i = outputContexts.values().iterator();
    while (i.hasNext()) {
      OutputDataStreamContext ctx = i.next();
      ctx.prepareCommit();
    }
    inputContext.prepareCommit();;
  }
  
  private void completeCommit() throws Exception {
    Iterator<OutputDataStreamContext> i = outputContexts.values().iterator();
    while (i.hasNext()) {
      OutputDataStreamContext ctx = i.next();
      ctx.completeCommit();
    }
    //The source should commit after sink commit. In the case the source or sink does not support
    //2 phases commit, it will cause the data to duplicate only, not loss
    inputContext.completeCommit();
  }
  
  public void commit() throws Exception {
    //prepareCommit is a vote to make sure both sink, invalidSink, and source
    //are ready to commit data, otherwise rollback will occur
    try {
      prepareCommit();
      completeCommit();
      report.updateCommit();
      workerService.getDataflowRegistry().getTaskRegistry().save(descriptor, report);
    } catch (Exception ex) {
      report.setCommitFailCount(report.getCommitFailCount() + 1);
      workerService.getLogger().warn("DataflowTask Commit Fail");
      throw ex;
    } 
  }
  
  public void rollback() throws Exception {
    //TODO: implement the proper transaction
    Iterator<OutputDataStreamContext> i = outputContexts.values().iterator();
    while (i.hasNext()) {
      OutputDataStreamContext ctx = i.next();
      ctx.rollback();
    }
    inputContext.rollback();
  }

  public void close() throws Exception {
    //TODO: implement the proper transaction
    Iterator<OutputDataStreamContext> i = outputContexts.values().iterator();
    while (i.hasNext()) {
      OutputDataStreamContext ctx = i.next();
      ctx.close();
      ;
    }
    inputContext.close();
  }
}
