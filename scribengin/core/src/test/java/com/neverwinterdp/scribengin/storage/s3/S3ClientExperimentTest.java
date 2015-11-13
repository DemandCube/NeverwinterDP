package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.yara.MetricPrinter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class S3ClientExperimentTest {
  static public String BUCKET_NAME = "s3-client-integration-test-" + UUID.randomUUID();
  
  static S3Client s3Client ;
  
  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client() ;
    s3Client.createBucket(BUCKET_NAME);
  }
  
  @AfterClass
  static public void afterClass() {
    s3Client.deleteBucket(BUCKET_NAME, true);
    s3Client.onDestroy();
  }
  
  @Test
  public void testS3ObjectReaderWriter() throws Exception {
    String KEY = "test-s3-object-writer" ;
    int    NUM_OF_RECORDS = 1000;
    MetricRegistry mRegistry = new MetricRegistry();
    long startTime = System.currentTimeMillis();
    Timer.Context writerCreateCtx = mRegistry.timer("writer.create").time();
    S3ObjectWriter writer = new S3ObjectWriter(s3Client, BUCKET_NAME, KEY, new ObjectMetadata());
    writerCreateCtx.stop();
    
    Timer writerWrite = mRegistry.timer("writer.write");
    for(int i = 0; i < NUM_OF_RECORDS; i++) {
      DataObj data = new DataObj("key-" + i, 512);
      Timer.Context writerWriteCtx = writerWrite.time(); 
      writer.write(JSONSerializer.INSTANCE.toBytes(data));
      writerWriteCtx.stop();
    }
    startTime = System.currentTimeMillis();
    Timer.Context writerCloseCtx = mRegistry.timer("writer.close").time();
    writer.waitAndClose(30 * NUM_OF_RECORDS);
    writerCloseCtx.stop();
    System.out.println("Write in: " + (System.currentTimeMillis() - startTime));
    
    for(int i = 0; i < 10; i++) {
      Timer.Context readerGetCtx = mRegistry.timer("reader.get").time();
      S3Object object = s3Client.getObject(BUCKET_NAME, KEY);
      readerGetCtx.stop();
      
      Timer.Context readerCreateCtx = mRegistry.timer("reader.create").time();
      S3ObjectReader reader = new S3ObjectReader(object);
      readerCreateCtx.stop();

      Timer readerRead = mRegistry.timer("reader.read");
      int count = 0 ;
      while(reader.hasNext()) {
        Timer.Context readerReadCtx = readerRead.time();
        byte[] data = reader.next();
        DataObj dataObj = JSONSerializer.INSTANCE.fromBytes(data, DataObj.class);
        readerReadCtx.stop();
        if(NUM_OF_RECORDS < 30) {
          System.out.println(dataObj.getKey());
        }
        count++ ;
      }
      reader.close();
      Assert.assertEquals(NUM_OF_RECORDS, count);
    }
    System.out.println("Read in: " + (System.currentTimeMillis() - startTime));
    MetricPrinter mPrinter = new MetricPrinter() ;
    mPrinter.print(mRegistry);
  }
  
  @Test
  public void testUpdateObjectMetadata() throws IOException, InterruptedException {
    String KEY = "test-update-object-metadata" ;
    ObjectMetadata metadata = new ObjectMetadata() ;
    metadata.setContentType("text/plain");
    s3Client.createObject(BUCKET_NAME, KEY, new byte[0], metadata);
    metadata = s3Client.getObjectMetadata(BUCKET_NAME, KEY) ;
    metadata.addUserMetadata("transaction", "buffering");
    s3Client.updateObjectMetadata(BUCKET_NAME, KEY, metadata);
    metadata = s3Client.getObjectMetadata(BUCKET_NAME, KEY) ;
    Assert.assertEquals("buffering", metadata.getUserMetaDataOf("transaction"));
  }
  
  @Test
  public void testGetRootFolders() {
    String folderPath= "unit-test";

    int counter = 3;
    for (int i = 0; i < counter; i++) {
      s3Client.createS3Folder(BUCKET_NAME, folderPath + "-" + i+ "/" + UUID.randomUUID());
    }

    List<S3Folder> folders = s3Client.getRootFolders(BUCKET_NAME);
    System.out.println("folders " + folders);
    assertEquals(counter, folders.size());
  }
  
  static public class DataObj {
    private String key ;
    private byte[] data ;

    public DataObj() {}
    
    public DataObj(String key, int size) {
      this.key = key;
      data = new byte[size];
    }
    
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    
    public byte[] getData() { return data; }
    public void setData(byte[] data) { this.data = data; }
  }
}