package com.neverwinterdp.scribengin.dataflow.log.sample;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaLogSampleWithWorkerKillUnitTest  {
  KafkaLogSampleRunner logSampleRunner = new KafkaLogSampleRunner();
  
  @Before
  public void setup() throws Exception {
    logSampleRunner.setup();
  }
  
  @After
  public void teardown() throws Exception {
    logSampleRunner.teardown();
  }
  
  @Test
  public void test() throws Exception {
    RunnerThread runnerThread = new RunnerThread();
    runnerThread.start();
    
    KillThread killThread = new KillThread() ;
    killThread.start();
    
    runnerThread.waitForTermination();
  }
 
  public class RunnerThread extends Thread {
    public void run() {
      try {
        logSampleRunner.runDataflow();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        synchronized(this) {
          notifyAll();
        }
      }
    }
    
    synchronized public void waitForTermination() throws InterruptedException {
      if(!isAlive()) return;
      wait();
    }
  }
  
  public class KillThread extends Thread {
    public void run() {
      try {
        String killCommand = 
            "dataflow kill-worker-random " +
            "  --dataflow-id " + logSampleRunner.dataflowId + 
            "  --wait-before-simulate-failure 5000 --failure-period 15000 --max-kill 3 --simulate-kill";
        logSampleRunner.shell.execute(killCommand);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        synchronized(this) {
          notifyAll();
        }
      }
    }
    
    synchronized public void waitForTermination() throws InterruptedException {
      if(!isAlive()) return;
      wait();
    }
  }
}