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
  private GeneratorThread generatorThread;
  private ValidatorThread validatorThread;
  
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient= shell.getVMClient();
    Registry registry = vmClient.getRegistry();
    
    generatorThread = new GeneratorThread(registry, dflBuilder.getTrackingConfig());
    generatorThread.start();
    
    submitDataflow(shell, dflBuilder.buildDataflow());
    
    validatorThread = new ValidatorThread(registry, dflBuilder.getTrackingConfig());
    validatorThread.start();
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
    Registry                registry;
    TrackingConfig          trackingConfig;
    ExtVMTMValidatorHDFSApp validatorApp;

    public ValidatorThread(Registry registry, TrackingConfig tConfig) {
      this.registry       = registry;
      this.trackingConfig = tConfig;
    }
    
    public void run() {
      validatorApp = new ExtVMTMValidatorHDFSApp();
      validatorApp.runValidate(registry, trackingConfig);
    }
  }
  
  static public class ExtVMTMValidatorHDFSApp extends VMTMValidatorHDFSApp {
    private AtomicInteger counter = new AtomicInteger();
    private HDFSStorageTag previousTag;
    
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
      storage.report(System.out);
      previousTag = tag;
      System.out.println("ExtVMTMValidatorHDFSApp: Finish runManagement(...)");
    }
  }
}
