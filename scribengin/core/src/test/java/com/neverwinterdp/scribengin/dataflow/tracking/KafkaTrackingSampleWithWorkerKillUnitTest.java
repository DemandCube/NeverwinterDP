package com.neverwinterdp.scribengin.dataflow.sample;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaTrackingSampleWithWorkerKillUnitTest  {
  TrackingSampleRunner trackingSampleRunner = new TrackingSampleRunner();
  
  @Before
  public void setup() throws Exception {
    trackingSampleRunner.setup();
    trackingSampleRunner.numOfMessagePerChunk = 10000;
    trackingSampleRunner.dataflowMaxRuntime = 90000;
  }
  
  @After
  public void teardown() throws Exception {
    trackingSampleRunner.teardown();
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
        trackingSampleRunner.submitVMTMGenrator();
        trackingSampleRunner.submitKafkaTMDataflow();
        trackingSampleRunner.submitKafkaVMTMValidator();
        trackingSampleRunner.runMonitor();
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
            "  --dataflow-id " + trackingSampleRunner.dataflowId + 
            "  --wait-before-simulate-failure 15000 --failure-period 15000 --max-kill 1 --simulate-kill";
        trackingSampleRunner.shell.execute(killCommand);
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