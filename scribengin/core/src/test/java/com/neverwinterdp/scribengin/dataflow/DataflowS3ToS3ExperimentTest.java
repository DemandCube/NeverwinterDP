package com.neverwinterdp.scribengin.dataflow;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.S3DataflowTest;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.io.FileUtil;

public class DataflowS3ToS3ExperimentTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected ScribenginClusterBuilder clusterBuilder;
  protected ScribenginShell shell;

  private S3Client s3Client;

  private String bucketName;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/storage", false);
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());

    bucketName = "s3-integration-test-" + UUID.randomUUID();

    s3Client = new S3Client();
  }

  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    if(s3Client.hasBucket(bucketName)) {
      System.out.println("DELETE BUCKET " + bucketName);
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.onDestroy();
  }

  @Test
  public void testDataflows() throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter(bucketName, "source", "sink", 10/*numOfStreams*/);
    submitter.start();
    submitter.waitForTermination(150000);
  }

  public class DataflowSubmitter extends Thread {
    private String bucketName;
    private String sourcePath;
    private String sinkPath;
    private int    numStreams;

    public DataflowSubmitter(String bucketName, String sourcePath, String sinkPath, int numStreams) {
      this.bucketName = bucketName;
      this.sourcePath = sourcePath;
      this.sinkPath   = sinkPath;
      this.numStreams = numStreams;
    }

    public void run() {
      try {
        String command =
            "dataflow-test " + S3DataflowTest.TEST_NAME +
            " --dataflow-id    s3-to-s3-1" +
            " --dataflow-name  s3-to-s3" +
            " --worker 2 --executor-per-worker 2" +
            " --duration 120000" +
            " --task-max-execute-time 15000" +
            " --source-auto-create-bucket"+
            " --source-location " + bucketName +
            " --source-name "     + sourcePath +
            " --source-num-of-stream " + numStreams +
            " --source-max-records-per-stream 500" +
            
            " --sink-location " + bucketName +
            " --sink-name "     + sinkPath +
            //" --sink-auto-delete-bucket"+
            
            " --print-dataflow-info -1" +
            " --debug-dataflow-task true" +
            " --debug-dataflow-worker true" +
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