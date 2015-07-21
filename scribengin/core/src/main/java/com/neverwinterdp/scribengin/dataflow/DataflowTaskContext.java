package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorService;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.yara.Meter;

public class DataflowTaskContext {
  private DataflowTaskExecutorService executorService;
  private DataflowTaskReport report;
  private SourceContext sourceContext;
  private Map<String, SinkContext> sinkContexts = new HashMap<String, SinkContext>();
  private DataflowTaskDescriptor dataflowTaskDescriptor;
  private boolean complete = false;
  private Meter dataflowReadMeter ;
  
  public DataflowTaskContext(DataflowTaskExecutorService service, DataflowTaskDescriptor descriptor, DataflowTaskReport report)  throws Exception {
    executorService = service;
    sourceContext = new SourceContext(service.getSourceFactory(), descriptor.getSourceStreamDescriptor());
    dataflowReadMeter = service.getMetricRegistry().getMeter("dataflow.source.throughput", "byte") ;
    Iterator<Map.Entry<String, StreamDescriptor>> i = descriptor.getSinkStreamDescriptors().entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<String, StreamDescriptor> entry = i.next();
        StreamDescriptor streamDescriptor = entry.getValue();
        SinkContext context = new SinkContext(service.getSinkFactory(), streamDescriptor);
        sinkContexts.put(entry.getKey(), context);
      }
      this.report = report;
      this.dataflowTaskDescriptor = descriptor;
  }

  public DataflowTaskReport getReport() { return this.report; }
  
  public SourceStreamReader getSourceStreamReader() { return sourceContext.assignedSourceStreamReader; }

  public boolean isComplete() { return this.complete ; }
  
  public void setComplete(boolean b) { this.complete = b ; }
  
  public boolean isEndOfDataStream() throws Exception { 
    return  getSourceStreamReader().isEndOfDataStream() ; 
  }
  
  public DataflowMessage nextRecord(long maxWaitForDataRead) throws Exception {
    DataflowMessage dataflowMessage = getSourceStreamReader().next(maxWaitForDataRead);
    if(dataflowMessage != null) {
      dataflowReadMeter.mark(dataflowMessage.getData().length + dataflowMessage.getKey().length());
    }
    return dataflowMessage ;
  }
  
  public void append(DataflowMessage dataflowMessage) throws Exception {
    SinkContext sinkContext = sinkContexts.get("default");
    sinkContext.assignedSinkStreamWriter.append(dataflowMessage);
    Meter meter = executorService.getMetricRegistry().getMeter("dataflow.sink.default.throughput"  , "byte") ;
    meter.mark(dataflowMessage.getData().length + dataflowMessage.getKey().length());
  }

  public void write(String sinkName, DataflowMessage dataflowMessage) throws Exception {
    SinkContext sinkContext = sinkContexts.get(sinkName);
    sinkContext.assignedSinkStreamWriter.append(dataflowMessage);
    Meter meter = executorService.getMetricRegistry().getMeter("dataflow.sink." + sinkName + ".throughput"  , "byte") ;
    meter.mark(dataflowMessage.getData().length + dataflowMessage.getKey().length());
  }
  
  public String[] getAvailableSinks() {
    String[] name = new String[sinkContexts.size()] ;
    sinkContexts.keySet().toArray(name) ;
    return name ;
  }
  
  public SinkContext getSinkContext(String name) {
    return sinkContexts.get(name) ;
  }
  
  private void prepareCommit() throws Exception {
    sourceContext.assignedSourceStreamReader.prepareCommit();
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.prepareCommit();
    }
  }

  public boolean commit() throws Exception {
    //prepareCommit is a vote to make sure both sink, invalidSink, and source
    //are ready to commit data, otherwise rollback will occur
    try {
      prepareCommit();
      completeCommit();
    } catch (Exception ex) {
      rollback();
      throw ex;
    }
    report.updateCommit();
    executorService.getDataflowRegistry().dataflowTaskReport(dataflowTaskDescriptor, report);
    return false;
  }

  private void rollback() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.rollback();
    }
    sourceContext.rollback();
  }

  public void close() throws Exception {
    //TODO: implement the proper transaction
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.close();
      ;
    }
    sourceContext.close();
  }

  private void completeCommit() throws Exception {
    Iterator<SinkContext> i = sinkContexts.values().iterator();
    while (i.hasNext()) {
      SinkContext ctx = i.next();
      ctx.completeCommit();
    }
    //The source should commit after sink commit. In the case the source or sink does not support
    //2 phases commit, it will cause the data to duplicate only, not loss
    sourceContext.assignedSourceStreamReader.completeCommit();
  }

  static public class SourceContext {
    private Source source;
    private SourceStream assignedSourceStream;
    private SourceStreamReader assignedSourceStreamReader;

    public SourceContext(SourceFactory factory, StreamDescriptor streamDescriptor) throws Exception {
      this.source = factory.create(streamDescriptor);
      this.assignedSourceStream = source.getStream(streamDescriptor.getId());
      this.assignedSourceStreamReader = assignedSourceStream.getReader("DataflowTask");
    }

    public void prepapreCommit() throws Exception {
      assignedSourceStreamReader.prepareCommit();
    }

    public void completeCommit() throws Exception {
      assignedSourceStreamReader.completeCommit();
    }

    public void commit() throws Exception {
      assignedSourceStreamReader.commit();
    }

    public void rollback() throws Exception {
      assignedSourceStreamReader.rollback();
    }

    public void close() throws Exception {
      assignedSourceStreamReader.close();
    }
  }

  static public class SinkContext {
    private Sink sink;
    private SinkStream assignedSinkStream;
    private SinkStreamWriter assignedSinkStreamWriter;

    public SinkContext(SinkFactory factory, StreamDescriptor streamDescriptor) throws Exception {
      sink = factory.create(streamDescriptor);
      assignedSinkStream = sink.getStream(streamDescriptor);
      assignedSinkStreamWriter = assignedSinkStream.getWriter();
    }

    public void prepareCommit() throws Exception {
      assignedSinkStreamWriter.prepareCommit();
    }

    public void completeCommit() throws Exception {
      assignedSinkStreamWriter.completeCommit();
    }

    public void commit() throws Exception {
      assignedSinkStreamWriter.commit();
    }

    public void rollback() throws Exception {
      assignedSinkStreamWriter.rollback();
    }

    public void close() throws Exception {
      assignedSinkStreamWriter.close();
    }
    
    public StreamDescriptor getStreamDescriptor() { return this.assignedSinkStream.getDescriptor() ;}
    
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("Sink:\n").
        append("  Type = ").append(sink.getDescriptor().getType()).
        append("  Stream Id = ").append(assignedSinkStream.getDescriptor().getId()).
        append("  Location = ").append(assignedSinkStream.getDescriptor().getLocation());
      return b.toString();
    }
    
  }
}