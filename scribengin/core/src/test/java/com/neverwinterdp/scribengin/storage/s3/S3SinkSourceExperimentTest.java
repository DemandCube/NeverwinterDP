package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class S3SinkSourceExperimentTest {

  private static S3Client s3Client;

  private String bucketName;
  private String folderName;

  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client();
    s3Client.onInit();
  }

  @AfterClass
  public static void afterClass() {
    s3Client.onDestroy();
  }

  @Before
  public void before() {
    bucketName = "sink-source-test-" + UUID.randomUUID();
    folderName = "integration-test";
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.createBucket(bucketName);
    s3Client.createS3Folder(bucketName, folderName);
  }

  @After
  public void after() {
    s3Client.deleteBucket(bucketName, true);
  }

  @Test
  public void testSinkSource() throws Exception {
    StorageDescriptor sinkDescriptor = new StorageDescriptor();
    sinkDescriptor.attribute("s3.bucket.name", bucketName);
    sinkDescriptor.attribute("s3.storage.path", folderName);

    S3Sink sink = new S3Sink(s3Client, sinkDescriptor);
    assertNotNull(sink.getSinkFolder());

    for(int i = 0; i < 2; i++) {
      SinkStream stream = sink.newStream();
      SinkStreamWriter writer = stream.getWriter();
      for (int j = 0; j < 100; j++) {
        String key = "stream=" + stream.getDescriptor().getId() + ",buffer=" + j + ",record=" + j;
        writer.append(Record.create(key, key));
      }
      writer.commit();
      writer.close();
    }
    SinkStream[] streams = sink.getStreams();
    assertEquals(2, streams.length);

    for (SinkStream sinkStream : streams) {
      assertNotNull(sinkStream.getDescriptor());
      assertNotNull(sinkStream.getWriter());
    }

    S3Util.listStructure(s3Client,bucketName);
    sink.close();
    
    StorageDescriptor sourceDescriptor = new StorageDescriptor("s3", bucketName);
    sourceDescriptor.attribute("s3.bucket.name", bucketName);
    sourceDescriptor.attribute("s3.storage.path", folderName);

    S3Source source = new S3Source(s3Client, sourceDescriptor);
    SourceStream[] sourceStreams = source.getStreams();
    assertEquals(2, streams.length);

    int recordCount = 0 ;
    for (int i = 0; i < streams.length; i++) {
      SourceStream stream = sourceStreams[i];
      SourceStreamReader reader = stream.getReader(stream.getDescriptor().getLocation());

      while (reader.next(1000) != null) {
        recordCount++;
      }

      reader.close();
      S3Util.listStructure(s3Client, bucketName);
    }
    int expected = sink.getStreams().length * 100;
    assertEquals(expected, recordCount);
  }
}