package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.client.VMClient;

public class KafkaGeneratorValidatorTrackingLauncher extends TestTrackingLauncher {
  
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient= shell.getVMClient();
    Registry registry = vmClient.getRegistry();
    
    GeneratorThread generatorThread = new GeneratorThread(registry, dflBuilder.getTrackingConfig());
    generatorThread.start();
    
    ValidatorThread validatorThread = new ValidatorThread(shell, dflBuilder);
    validatorThread.start();
    
    while(validatorThread.isAlive()) {
      Thread.sleep(1000);
    }
  }
  
  static public class ValidatorThread extends Thread {
    ScribenginShell         shell;
    Registry                registry;
    TrackingDataflowBuilder dflBuilder;
    VMTMValidatorKafkaApp   validatorApp;

    public ValidatorThread(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) {
      this.shell          = shell ;
      this.registry       = shell.getVMClient().getRegistry();
      this.dflBuilder     = dflBuilder;
    }
    
    public void run() {
      validatorApp = new VMTMValidatorKafkaApp();
      validatorApp.runValidate(registry, dflBuilder.getTrackingConfig());
    }
  }
}