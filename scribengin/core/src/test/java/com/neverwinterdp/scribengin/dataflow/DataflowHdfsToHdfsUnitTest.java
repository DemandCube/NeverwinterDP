package com.neverwinterdp.scribengin.dataflow;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.HDFSDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.io.FileUtil;

public class DataflowHdfsToHdfsUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected ScribenginClusterBuilder clusterBuilder;
  protected ScribenginShell shell;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/storage", false);
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }

  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }

  @Test
  public void testDataflows() throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter();
    submitter.start();
    submitter.waitForTermination(120000);
  }

  public class DataflowSubmitter extends Thread {
    public void run() {
      try {
        String command =
            "dataflow-test " + HDFSDataflowTest.TEST_NAME +
                " --dataflow-name  hdfs-to-hdfs" +
                " --worker 2" +
                " --executor-per-worker 2" +
                " --duration 90000" +
                " --task-max-execute-time 1000" +
                " --source-name input" +
                " --source-num-of-stream 10" +
                " --source-write-period 0" +
                " --source-max-records-per-stream 1000" +
                " --sink-name output" +
                " --print-dataflow-info -1" +
                " --debug-dataflow-task " +
                " --debug-dataflow-vm " +
                " --debug-dataflow-activity " +
                " --junit-report build/junit-report.xml" +
                " --dump-registry";
        shell.execute(command);
      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        notifyTermimation();
      }
    }

    synchronized void notifyTermimation() {
      notify();
    }

    synchronized void waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
    }
  }
}