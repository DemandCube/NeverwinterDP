package com.neverwinterdp.scribengin.dataflow.tracking;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.hdfs.HDFSStorage;
import com.neverwinterdp.storage.hdfs.HDFSStorageTag;
import com.neverwinterdp.vm.client.VMClient;

public class HDFSTaggingTestLauncher extends TrackingTestLauncher {
  private boolean waitForValidator = true;
  
  private GeneratorThread generatorThread;
  private ValidatorThread validatorThread;
  
  public void setWaitForValidator(boolean b) { 
    waitForValidator = b;
  }
  
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient= shell.getVMClient();
    Registry registry = vmClient.getRegistry();
    
    generatorThread = new GeneratorThread(registry, dflBuilder.getTrackingConfig());
    generatorThread.start();
    
    submitDataflow(shell, dflBuilder.buildDataflow());
    
    Thread.sleep(90000);
    
    validatorThread = new ValidatorThread(shell, dflBuilder);
    validatorThread.start();
    
    while(validatorThread.isAlive()) {
      Thread.sleep(1000);
    }
  }
  
  public void onDestroy() throws InterruptedException {
    if(generatorThread.isAlive()) generatorThread.interrupt();
    if(validatorThread.isAlive()) validatorThread.interrupt();
    while(generatorThread.isAlive() || validatorThread.isAlive()) {
      Thread.sleep(250);
    }
  }
  
  static public class GeneratorThread extends Thread {
    private Registry       registry;
    private TrackingConfig trackingConfig;
    private VMTMGeneratorKafkaApp generatorApp;
    
    public GeneratorThread(Registry registry, TrackingConfig tConfig) {
      this.registry       = registry;
      this.trackingConfig = tConfig;
    }
    
    public void run() {
      generatorApp = new VMTMGeneratorKafkaApp();
      generatorApp.runGenerator(registry, trackingConfig);
    }
  }
  
  static public class ValidatorThread extends Thread {
    ScribenginShell         shell;
    Registry                registry;
    TrackingDataflowBuilder dflBuilder;
    ExtVMTMValidatorHDFSApp validatorApp;

    public ValidatorThread(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) {
      this.shell          = shell ;
      this.registry       = shell.getVMClient().getRegistry();
      this.dflBuilder     = dflBuilder;
    }
    
    public void run() {
      validatorApp = new ExtVMTMValidatorHDFSApp(shell, dflBuilder);
      validatorApp.runValidate(registry, dflBuilder.getTrackingConfig());
    }
  }
  
  static public class ExtVMTMValidatorHDFSApp extends VMTMValidatorHDFSApp {
    private ScribenginShell shell;
    private TrackingDataflowBuilder dflBuilder;
    private AtomicInteger   counter = new AtomicInteger();
    private HDFSStorageTag  previousTag;
    
    ExtVMTMValidatorHDFSApp(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) {
      this.shell = shell;
      this.dflBuilder = dflBuilder;
    }
    
    protected void runManagement(HDFSStorage storage) throws RegistryException, IOException {
      System.out.println("ExtVMTMValidatorHDFSApp: Start runManagement(...)");
      storage.doManagement();
      
      int tagId = counter.incrementAndGet();
      HDFSStorageTag tag = null ;
      if(tagId % 2 ==  1) {
        tag = storage.findTagByDateTime("tag-by-time-" + tagId, "Tag by the current time", new Date()) ;
      } else {
        tag = storage.findTagByRecordLastPosition("tag-by-latest-position", "Tag by the latest position");
      }
      storage.createTag(tag);
      
      if(previousTag != null) {
        storage.cleanDataByTag(previousTag);
      }
      previousTag = tag;
      try {
        shell.execute(
            "plugin com.neverwinterdp.scribengin.dataflow.tracking.TrackingMonitor" +
            "  --dataflow-id " + dflBuilder.getDataflowId()  +  
            " --report-path "  + dflBuilder.getTrackingConfig().getTrackingReportPath() //+ " --show-history-vm "
        );
      } catch (Exception e) {
        e.printStackTrace();
      }
      storage.report(System.out);
    }
  }
}
