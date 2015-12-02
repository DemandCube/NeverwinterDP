package com.neverwinterdp.storage.s3;

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
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.storage.s3.S3Folder;
import com.neverwinterdp.storage.s3.S3ObjectReader;
import com.neverwinterdp.storage.s3.S3ObjectWriter;
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
    int    NUM_OF_RECORDS = 100;
    
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
    
    try {
      startTime = System.currentTimeMillis();
      Timer.Context writerCloseCtx = mRegistry.timer("writer.close").time();
      writer.waitAndClose(5000);
      writerCloseCtx.stop();
    } catch(Throwable t) {
      t.printStackTrace();
    }
    System.err.println("Write in: " + (System.currentTimeMillis() - startTime));
    MetricPrinter mPrinter = new MetricPrinter() ;
    for(int i = 0; i < 1; i++) {
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
      Thread.sleep(1000);
      reader.dump();
      reader.close();
      object.close();
      mPrinter.print(mRegistry);
      Assert.assertEquals(NUM_OF_RECORDS, count);
    }
    System.out.println("Read in: " + (System.currentTimeMillis() - startTime));
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
  public void testDeleteWithPrefix() throws IOException, InterruptedException {
    int NUM_OF_FILES = 10;
    String FOLDER = "folder" ;
    long start = System.currentTimeMillis();
    for(int i = 0; i < NUM_OF_FILES; i++) {
      ObjectMetadata metadata = new ObjectMetadata() ;
      metadata.setContentType("text/plain");
      s3Client.createObject(BUCKET_NAME, FOLDER + "/" + i, new byte[128], metadata);
      if(i % 100 == 0) {
        System.out.println("create " + (i + 1) + " in " + (System.currentTimeMillis() - start) + "ms");
      }
    }
    List<String> children = s3Client.listKeyChildren(BUCKET_NAME, FOLDER, "/");
    Assert.assertEquals(NUM_OF_FILES, children.size());
    s3Client.deleteKeyWithPrefix(BUCKET_NAME, FOLDER);
    Assert.assertEquals(0, s3Client.listKeyWithPrefix(BUCKET_NAME, FOLDER).size());
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