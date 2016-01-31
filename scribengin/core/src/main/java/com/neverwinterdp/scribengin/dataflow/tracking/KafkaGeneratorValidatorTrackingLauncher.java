package com.neverwinterdp.scribengin.dataflow.tracking;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.vm.client.VMClient;

public class KafkaGeneratorValidatorTrackingLauncher extends TestTrackingLauncher {
  @Parameter(names = "--validator-launch-delay", description = "")
  private long validatorLaunchDelay = 300000;
  
  @Override
  public void execute(ScribenginShell shell, TrackingDataflowBuilder dflBuilder) throws Exception {
    VMClient vmClient= shell.getVMClient();
    Registry registry = vmClient.getRegistry();
    
    GeneratorThread generatorThread = new GeneratorThread(registry, dflBuilder.getTrackingConfig());
    generatorThread.start();
    
    shell.console().println("The validator will be launched in " + validatorLaunchDelay + "ms, you need to manually launch the dataflow now");
    Thread.sleep(validatorLaunchDelay);
    
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