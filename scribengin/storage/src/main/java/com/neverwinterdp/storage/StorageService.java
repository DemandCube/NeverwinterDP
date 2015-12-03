package com.neverwinterdp.storage;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.storage.kafka.KafkaStorage;
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.storage.s3.S3Storage;
import com.neverwinterdp.storage.simplehdfs.SimpleHDFSStorage;

@Singleton
public class StorageService {
  @Inject
  private FileSystem fs;
  
  @Inject
  private KafkaClient kafkaClient;
  
  @Inject
  private S3Client s3Client;
  
  private Map<String, KafkaStorage> cacheKafkaStorage = new HashMap<>();
  private Map<String, SimpleHDFSStorage>  cacheSimpleHDFSStorage = new HashMap<>();
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
    } else if("simplehdfs".equalsIgnoreCase(storageConfig.getType())) {
      String location  = storageConfig.getLocation();
      String key = "simplehdfs:" + location;
      SimpleHDFSStorage storage = cacheSimpleHDFSStorage.get(key);
      if(storage == null) {
        storage = new SimpleHDFSStorage(fs, storageConfig);
        cacheSimpleHDFSStorage.put(key, storage);
      }
      return storage;
    } else if("s3".equalsIgnoreCase(storageConfig.getType())){
      String bucket = storageConfig.attribute(S3Storage.BUCKET_NAME);
      String storagePath = storageConfig.attribute(S3Storage.STORAGE_PATH);
      String key = bucket + ":" + storagePath;
      S3Storage storage = cacheS3Storage.get(key);
      if(storage == null) {
        storage = new S3Storage(s3Client, storageConfig);
        cacheS3Storage.put(key, storage);
      }
      return storage;
    }
    throw new Exception("Unknown sink type " + storageConfig.getType());
  }
}