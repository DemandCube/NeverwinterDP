package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class DataflowTask {
  private DataflowContainer container;
  private final TaskContext<DataflowTaskDescriptor> taskContext;
  private DataflowTaskDescriptor descriptor ;
  private ScribeAbstract processor;
  private DataflowTaskContext context;
  private boolean interrupt = false;
  private boolean complete = false;
  private long    startTime = 0;
  
  public DataflowTask(DataflowContainer container, TaskContext<DataflowTaskDescriptor> taskContext) throws Exception {
    this.container = container;
    this.taskContext = taskContext;
    this.descriptor = taskContext.getTaskDescriptor(true);
    Class<ScribeAbstract> scribeType = (Class<ScribeAbstract>) Class.forName(descriptor.getScribe());
    processor = scribeType.newInstance();
  }
  
  public TaskContext<DataflowTaskDescriptor> getTaskContext() { return this.taskContext; }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public boolean isComplete() { return this.complete ; }
  
  public void interrupt() { interrupt = true; }
  
  public void init() throws Exception {
    startTime = System.currentTimeMillis();
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    DataflowTaskReport report = dRegistry.getTaskReport(descriptor);
    report.incrAssignedCount();
    dRegistry.dataflowTaskReport(descriptor, report);
    context = new DataflowTaskContext(container, descriptor, report);
  }
  
  public void run() throws Exception {
    DataflowTaskReport report = context.getReport();
    SourceStreamReader reader = context.getSourceStreamReader() ;
    Record record = null ;
    DataflowDescriptor dflDescriptor =   container.getDataflowRegistry().getDataflowDescriptor(false);
    long maxWaitForDataRead =  dflDescriptor.getMaxWaitForDataRead();
    while(!interrupt && (record = reader.next(maxWaitForDataRead)) != null) {
      report.incrProcessCount();
      processor.process(record, context);
    }
    if(record == null) complete = true;
  }
  
  public void suspend() throws Exception {
    saveContext();
    container.getDataflowRegistry().dataflowTaskSuspend(taskContext);
  }
  
  public void finish() throws Exception {
    DataflowTaskReport report = context.getReport();
    report.setFinishTime(System.currentTimeMillis());
    saveContext();
    container.getDataflowRegistry().dataflowTaskFinish(taskContext);
  }
  
  void saveContext() throws Exception {
    DataflowTaskReport report = context.getReport();
    report.addAccRuntime(System.currentTimeMillis() - startTime);
    context.commit();
    context.close();
  }
}