package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.registry.WorkerRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class TrackingJUnitShellPlugin extends SubCommand {
  @Parameter(names = "--dataflow-id", required=true, description = "The dataflow id")
  private String dataflowId ;
  
  @Parameter(names = "--report-path", required=true, description = "The report path in the registry")
  private String reportPath ;
  
  @Parameter(names = "--junit-report-file", description = "Pring the report")
  private String junitReportFile = null;
  
  @Override
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    ScribenginShell scribenginShell = (ScribenginShell) shell;
    
    DataflowClient dflClient = scribenginShell.getScribenginClient().getDataflowClient(dataflowId);
    DataflowLifecycleStatus status = dflClient.getStatus();
    
    JUnitTestSetResult testSetResult = new JUnitTestSetResult();
    testSetResult.add("Expect the dataflow TERMINATED status, status = " + status, status == DataflowLifecycleStatus.TERMINATED);
    
    WorkerRegistry workerRegistry = dflClient.getDataflowRegistry().getWorkerRegistry();
    List<String> allWorkerIds = workerRegistry.getAllWorkerIds();
    for(String workerId : allWorkerIds) {
      DataflowWorkerStatus workerStatus = workerRegistry.getDataflowWorkerStatus(workerId);
      testSetResult.add("Expect worker " + workerId + " TERMINATED status, status = " + workerStatus, workerStatus == DataflowWorkerStatus.TERMINATED);
    }
    
    TrackingRegistry trackingRegistry = new TrackingRegistry(scribenginShell.getVMClient().getRegistry(), reportPath, false);
    Map<String, TrackingMessageReport> validatedMessageReportMap = new HashMap<>();
    for(TrackingMessageReport sel : trackingRegistry.getValidatorReports()) {
      validatedMessageReportMap.put(sel.getChunkId(), sel);
    }
    
    for(TrackingMessageReport generatedReport : trackingRegistry.getGeneratorReports()) {
      TrackingMessageReport validatedReport = validatedMessageReportMap.get(generatedReport.getChunkId()) ;
      if(validatedReport == null) {
        StringBuilder b = new StringBuilder();
        b.append("Chunk " + generatedReport.getChunkId()).append(", ").
          append("generate = ").append(generatedReport.getProgress()).append(", ").
          append("validate = ").append(0).append(", ").
          append("loss = ").append(generatedReport.getProgress()).append(", ").
          append("duplicated = ").append(0);
        testSetResult.add(b.toString() , false);
      } else {
        boolean ok = generatedReport.getProgress() == validatedReport.getProgress() && validatedReport.getLostCount() == 0;
        StringBuilder b = new StringBuilder();
        b.append("Chunk " + generatedReport.getChunkId()).append(", ").
          append("generate = ").append(generatedReport.getProgress()).append(", ").
          append("validate = ").append(validatedReport.getProgress()).append(", ").
          append("loss = ").append(validatedReport.getLostCount()).append(", ").
          append("duplicated = ").append(validatedReport.getDuplicatedCount());
        testSetResult.add(b.toString() , ok);
      }
    }
    
    if(junitReportFile  != null) {
      testSetResult.writeReportTo(junitReportFile);
    } else {
      shell.console().println("Need to specify the report file by adding the [--junit-report-file path] parameter");
      shell.console().println(testSetResult.getXmlResult());
    }
  }
  
  @Override
  public String getDescription() { return "Tracking Monitor command"; }
  
  static public class JUnitTestSetResult {
    private TestSet testResultSet = new TestSet();
    private int     currentTestId = 0 ;
   
    public void add(String desc, boolean passed) {
      TestResult testResult = null;
      if (passed) {
        testResult = new TestResult(StatusValues.OK, ++currentTestId);
      } else {
        testResult = new TestResult(StatusValues.NOT_OK, ++currentTestId);
      }
      testResult.setDescription(desc);
      testResultSet.addTestResult(testResult);
    }
    
    public String getXmlResult() {
      TapProducer tapProducer = TapProducerFactory.makeTapJunitProducer(getClass().getSimpleName());
      String formattedText = tapProducer.dump(testResultSet);
      return formattedText;
    }
    
    public void writeReportTo(String path) throws IOException {
      File file = new File(path);
      file.createNewFile();
      TapProducer tapProducer = TapProducerFactory.makeTapJunitProducer(TrackingJUnitShellPlugin.class.getSimpleName());
      tapProducer.dump(testResultSet, file);
    }
  }
}
