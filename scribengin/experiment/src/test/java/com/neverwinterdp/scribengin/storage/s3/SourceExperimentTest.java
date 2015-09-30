package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.tool.message.MessageExtractor;
import com.neverwinterdp.tool.message.MessageTracker;

/**
 * @author Anthony Musyoki
 */

public class SourceExperimentTest {
  private static S3Client s3Client;

  private String bucketName;
  private String folderPath;
  private int numOfBuffersPerStream =5;
  private int numRecordsPerBuffer = 100;

  @BeforeClass
  public static void setupClass() {
    s3Client = new S3Client();
  }

  @AfterClass
  public static void tearDownClass() {
    s3Client.onDestroy();
  }

  @Before
  public void setup() throws Exception {
    bucketName = "source-experimenttest-" + UUID.randomUUID();
    folderPath ="data-folder-1";
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.createBucket(bucketName);
    new S3SourceGenerator().generateSource(s3Client, bucketName, folderPath, numOfBuffersPerStream, numRecordsPerBuffer);
  }

  @After
  public void teardown() throws Exception {
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
  }

  
  @Test
  public void testSource() throws Exception {
    S3Util.listStructure(s3Client, bucketName);

    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderPath);
    S3Source source = new S3Source(s3Client, descriptor);

    MessageTracker messageTracker = new MessageTracker();
    MessageExtractor messageExtractor = MessageExtractor.DEFAULT_MESSAGE_EXTRACTOR;

    SourcePartitionStream[] streams = source.getStreams();

    assertEquals(numOfBuffersPerStream, streams.length);
    for (SourcePartitionStream stream : streams) {
      SourcePartitionStreamReader reader = stream.getReader("test");
      Record dataflowMessage;
      while ((dataflowMessage = reader.next(1000)) != null) {
        Message message = messageExtractor.extract(dataflowMessage.getData());
        messageTracker.log(message);
      }
      reader.close();
    }
    messageTracker.optimize();
    messageTracker.dump(System.out);
    int totalRecords = numOfBuffersPerStream * numRecordsPerBuffer;
    assertEquals(totalRecords, messageTracker.getLogCount());
    assertTrue(messageTracker.isInSequence());
    S3Util.listStructure(s3Client, bucketName);
  }
}
