package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class S3SinkSourceExperimentTest {

  private static S3Client s3Client;

  private String bucketName;
  private String storageFolder;

  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client();
  }

  @AfterClass
  public static void afterClass() {
    s3Client.onDestroy();
  }

  @Before
  public void before() {
    bucketName = "sink-source-test-" + UUID.randomUUID();
    storageFolder = "integration-test";
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.createBucket(bucketName);
    s3Client.createS3Folder(bucketName, storageFolder);
  }

  @After
  public void after() {
    s3Client.deleteBucket(bucketName, true);
  }

  @Test
  public void testSinkSource() throws Exception {
    S3Storage storage = new S3Storage(bucketName, storageFolder);
    S3Sink sink = storage.getSink(s3Client) ;
    assertNotNull(sink.getSinkFolder());
    int NUM_MESSAGE_PER_STREAM = 1500;
    for(int i = 0; i < 2; i++) {
      SinkPartitionStream stream = sink.newStream();
      SinkPartitionStreamWriter writer = stream.getWriter();
      for (int j = 0; j < NUM_MESSAGE_PER_STREAM; j++) {
        String key = "stream=" + stream.getDescriptor().getPartitionId() + ",buffer=" + j + ",record=" + j;
        key = key + key + key + key + key + key + key + key + key + key + key + key + key + key + key + key + key + key;
        writer.append(Record.create(key, key));
        if((j + 1) % 1000 == 0) {
          writer.commit();
        }
      }
      writer.commit();
      writer.close();
    }
    SinkPartitionStream[] streams = sink.getStreams();
    assertEquals(2, streams.length);

    for (SinkPartitionStream sinkStream : streams) {
      assertNotNull(sinkStream.getDescriptor());
      assertNotNull(sinkStream.getWriter());
    }

    S3Util.listStructure(s3Client, bucketName);
    sink.close();
    
    S3Source source = storage.getSource(s3Client);
    SourcePartitionStream[] sourceStreams = source.getStreams();
    assertEquals(2, streams.length);

    int recordCount = 0 ;
    for (int i = 0; i < streams.length; i++) {
      SourcePartitionStream stream = sourceStreams[i];
      SourcePartitionStreamReader reader = stream.getReader(stream.getDescriptor().getLocation());

      while (reader.next(1000) != null) {
        recordCount++;
      }
      reader.close();
    }
    S3Util.listStructure(s3Client, bucketName);
    int expected = sink.getStreams().length * NUM_MESSAGE_PER_STREAM;
    assertEquals(expected, recordCount);
  }
}