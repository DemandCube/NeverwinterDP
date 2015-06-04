package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.storage.s3.S3Client;

public class S3DataflowTest extends DataflowTest {

  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new S3DataflowSourceGenerator();

  @ParametersDelegate
  private DataflowSinkValidator sinkValidator = new S3DataflowSinkValidator();

  private S3Client s3Client;

  final static public String TEST_NAME = "s3-to-s3";

  @Parameter(names = "--source-auto-create-bucket", description = "Enable auto creating a bucket")
  protected boolean sourceAutoCreateBucket = false;

  @Parameter(names = "--sink-auto-delete-bucket", description = "Enable auto deleting a bucket")
  protected boolean sinkAutoDeleteBucket = false;

  protected void doRun(ScribenginShell shell) throws Exception {
    s3Client = new S3Client();
    s3Client.onInit();
    
    if (sourceAutoCreateBucket) {
      s3Client.createBucket(sourceGenerator.sourceLocation);
    }

    sourceToSinkDataflowTest(shell, sourceGenerator, sinkValidator);

    if (sinkAutoDeleteBucket) {
      s3Client.deleteBucket(sinkValidator.sinkLocation, true);
    }

    s3Client.onDestroy();
  }
}