package com.neverwinterdp.dataflow.logsample.vm;

import java.util.Random;

import org.slf4j.Logger;

import com.neverwinterdp.dataflow.logsample.LogMessageGenerator;
import com.neverwinterdp.dataflow.logsample.LogSampleRegistry;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMDescriptor;

public class VMLogMessageGeneratorApp extends VMApp {
  
  @Override
  public void run() throws Exception {
    Logger logger = getVM().getLoggerFactory().getLogger("LogSample") ;
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    int numOfMessage = vmDescriptor.getVmConfig().getPropertyAsInt("num-of-message", 5000);
    int messageSize = vmDescriptor.getVmConfig().getPropertyAsInt("message-size", 256);
    LogMessageGenerator logMessageGenerator = new LogMessageGenerator(getVM().getDescriptor().getId(), messageSize) ;
    Random rand = new Random() ;
    for(int i = 0; i < numOfMessage; i++) {
      double randNum = rand.nextDouble();
      if(randNum < 0.4) {
        logger.info(logMessageGenerator.nextMessageAsJson());
      } else if (randNum < 0.7) {
        logger.warn(logMessageGenerator.nextMessageAsJson());
      } else {
        logger.error(logMessageGenerator.nextMessageAsJson());
      }
    }
    Thread.sleep(5000);
    
    LogSampleRegistry appRegistry = new LogSampleRegistry(getVM().getVMRegistry().getRegistry());
    appRegistry.addGenerateReport(logMessageGenerator.getLogMessageReport());
  }
}