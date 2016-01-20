package com.neverwinterdp.scribengin.dataflow.tracking;

import java.io.IOException;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.hdfs.HDFSStorage;
import com.neverwinterdp.vm.client.VMClient;

public class HDFSTaggingTestLauncher extends TrackingTestLauncher {
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient= shell.getVMClient();
    Registry registry = vmClient.getRegistry();
    GeneratorThread generatorThread = new GeneratorThread(registry, dflBuilder.getTrackingConfig());
    generatorThread.start();
    
    submitDataflow(shell, dflBuilder.buildDataflow());
    
    ValidatorThread validatorThread = new ValidatorThread(registry, dflBuilder.getTrackingConfig());
    validatorThread.start();
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
    
    protected void runManagement(HDFSStorage storage) throws RegistryException, IOException {
      storage.getRegistry();
    }
  }
}
