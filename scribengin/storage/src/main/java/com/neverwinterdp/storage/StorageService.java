package com.neverwinterdp.storage;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.storage.es.ESStorage;
import com.neverwinterdp.storage.es.ESStorageConfig;
import com.neverwinterdp.storage.hdfs.HDFSStorage;
import com.neverwinterdp.storage.kafka.KafkaStorage;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.storage.nulldev.NullDevStorage;
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.storage.s3.S3Storage;
import com.neverwinterdp.storage.simplehdfs.SimpleHDFSStorage;

@Singleton
public class StorageService {
  @Inject
  private Registry registry;
  
  @Inject
  private FileSystem fs;
  
  @Inject
  private KafkaClient kafkaClient;
  
  @Inject
  private S3Client s3Client;
  
  private Map<String, KafkaStorage>      cacheKafkaStorage      = new HashMap<>();
  private Map<String, HDFSStorage>       cacheHDFSStorage       = new HashMap<>();
  private Map<String, SimpleHDFSStorage> cacheSimpleHDFSStorage = new HashMap<>();
  private Map<String, S3Storage>         cacheS3Storage         = new HashMap<>();
  
  public StorageService() {
  }
  
  public FileSystem getFileSyztem() { return this.fs ; }

  synchronized public Storage getStorage(StorageConfig storageConfig) throws Exception {
    if("kafka".equalsIgnoreCase(storageConfig.getType())) {
      KafkaStorageConfig kStorageConfig = new KafkaStorageConfig(storageConfig);
      String zkConnect = kStorageConfig.getZKConnect();
      String topic     = kStorageConfig.getTopic();
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
        storage = new HDFSStorage(registry, fs, storageConfig);
        cacheHDFSStorage.put(key, storage);
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
    } else if("es".equalsIgnoreCase(storageConfig.getType())) {
      ESStorageConfig esStorageConfig = new ESStorageConfig(storageConfig);
      return new ESStorage(esStorageConfig);
    } else if("nulldev".equalsIgnoreCase(storageConfig.getType())) {
      return new NullDevStorage(storageConfig);
    }
    throw new Exception("Unknown sink type " + storageConfig.getType());
  }
}