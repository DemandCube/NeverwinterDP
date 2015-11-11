package com.neverwinterdp.scribengin.storage;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.hdfs.HDFSStorage;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;

@Singleton
public class StorageService {
  @Inject
  private FileSystem fs;
  
  @Inject
  private KafkaClient kafkaClient;
  
  @Inject
  private S3Client s3Client;
  
  private Map<String, KafkaStorage> cacheKafkaStorage = new HashMap<>();
  private Map<String, HDFSStorage>  cacheHDFSStorage = new HashMap<>();
  private Map<String, S3Storage>  cacheS3Storage = new HashMap<>();
  
  public StorageService() {
  }
  
  public FileSystem getFileSyztem() { return this.fs ; }

  synchronized public Storage getStorage(StorageConfig storageConfig) throws Exception {
    if("kafka".equalsIgnoreCase(storageConfig.getType())) {
      String zkConnect = storageConfig.attribute(KafkaStorage.ZK_CONNECT);
      String topic     = storageConfig.attribute(KafkaStorage.TOPIC);
      String key = "kafka:" + zkConnect + "/" + topic;
      KafkaStorage storage = cacheKafkaStorage.get(key);
      if(storage == null) {
        storage = new KafkaStorage(kafkaClient, storageConfig);
        cacheKafkaStorage.put(key, storage);
      }
      return storage;
    } else if("hdfs".equalsIgnoreCase(storageConfig.getType())) {
      String location  = storageConfig.getLocation();
      String key = "hdfs:" + location;
      HDFSStorage storage = cacheHDFSStorage.get(key);
      if(storage == null) {
        storage = new HDFSStorage(fs, storageConfig);
        cacheHDFSStorage.put(key, storage);
      }
      return storage;
    } else if("s3".equalsIgnoreCase(storageConfig.getType())){
      String bucket = storageConfig.attribute(S3Storage.BUCKET);
      String folder = storageConfig.attribute(S3Storage.FOLDER);
      
      S3Storage storage = cacheS3Storage.get(bucket);
      if(storage == null) {
        storage = new S3Storage(bucket, folder);
        cacheS3Storage.put(bucket, storage);
      }
      return storage;
    }
    throw new Exception("Unknown sink type " + storageConfig.getType());
  }
}