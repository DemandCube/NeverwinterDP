package com.neverwinterdp.scribengin.storage;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.kafka.KafkaStorage;
import com.neverwinterdp.scribengin.storage.s3.S3Client;

@Singleton
public class StorageService {
  @Inject
  private FileSystem fs;
  
  @Inject
  private KafkaClient kafkaClient;
  
  @Inject
  private S3Client s3Client;
  
  private Map<String, KafkaStorage> cacheKafkaStorage = new HashMap<>();
  
  public StorageService() {
  }
  
  public StorageService(FileSystem fs, S3Client s3Client) {
    this.fs = fs;
    this.s3Client = s3Client;
  }
  
  public FileSystem getFileSyztem() { return this.fs ; }

  synchronized public Storage getStorage(StorageConfig storageConfig) throws Exception {
    if("kafka".equalsIgnoreCase(storageConfig.getType())) {
      String zkConnect = storageConfig.attribute(KafkaStorage.ZK_CONNECT);
      String topic     = storageConfig.attribute(KafkaStorage.TOPIC);
      String key = zkConnect + "/" + topic;
      KafkaStorage storage = cacheKafkaStorage.get(key);
      if(storage == null) {
        storage = new KafkaStorage(kafkaClient, storageConfig);
        cacheKafkaStorage.put(key, storage);
      }
      return storage;
    }
    throw new Exception("Unknown sink type " + storageConfig.getType());
  }
}