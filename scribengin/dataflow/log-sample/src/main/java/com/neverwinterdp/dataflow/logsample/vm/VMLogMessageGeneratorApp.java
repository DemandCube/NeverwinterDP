package com.neverwinterdp.dataflow.logsample.vm;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.dataflow.logsample.LogMessageReport;
import com.neverwinterdp.dataflow.logsample.LogSampleRegistry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.tool.message.MessageGenerator;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMDescriptor;

public class VMLogMessageGeneratorApp extends VMApp {
  int numOfMessagePerExecutor ;
  int messageSize;
  
  @Override
  public void run() throws Exception {
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    int numOfExecutor = vmDescriptor.getVmConfig().getPropertyAsInt("num-of-executor", 1);
    numOfMessagePerExecutor = vmDescriptor.getVmConfig().getPropertyAsInt("num-of-message-per-executor", 5000);
    messageSize = vmDescriptor.getVmConfig().getPropertyAsInt("message-size", 256);
    
    ExecutorService executorService = Executors.newFixedThreadPool(numOfExecutor);

    for(int i = 0; i < numOfExecutor; i++) {
      String vmId = getVM().getDescriptor().getId();
      String groupId = vmId + "-executor-" + (i + 1);
      executorService.submit(new RunnableLogMessageGenerator(groupId));
    }
    executorService.shutdown();
    executorService.awaitTermination(60, TimeUnit.MINUTES);
    Thread.sleep(15000);
  }
  
  public class RunnableLogMessageGenerator implements Runnable {
    private String groupId ;
    
    public  RunnableLogMessageGenerator(String groupId) {
      this.groupId = groupId;
    }
    
    @Override
    public void run() {
      Logger logger = getVM().getLoggerFactory().getLogger("LogSample") ;
      
      MessageGenerator messageGenerator = new MessageGenerator.DefaultMessageGenerator() ;
      Random rand = new Random() ;
      for(int i = 0; i < numOfMessagePerExecutor; i++) {
        double randNum = rand.nextDouble();
        String jsonMessage = new String(messageGenerator.nextMessage(groupId, messageSize)) ;
        if(randNum < 0.4) {
          logger.info(jsonMessage);
        } else if (randNum < 0.7) {
          logger.warn(jsonMessage);
        } else {
          logger.error(jsonMessage);
        }
      }
      LogSampleRegistry appRegistry = null;
      try {
        appRegistry = new LogSampleRegistry(getVM().getVMRegistry().getRegistry());
        LogMessageReport report = new LogMessageReport(groupId, messageGenerator.getCurrentSequenceId(groupId)) ;
        appRegistry.addGenerateReport(report);
      } catch (RegistryException e) {
        if(appRegistry != null) {
          try {
            appRegistry.addGenerateError(groupId, e);
          } catch (RegistryException error) {
            error.printStackTrace();
          }
        }
        e.printStackTrace();
      }
    }
  }
}