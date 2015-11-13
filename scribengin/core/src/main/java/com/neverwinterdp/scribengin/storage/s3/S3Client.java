package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.beust.jcommander.Parameter;
import com.google.inject.Singleton;

@Singleton
public class S3Client {
  private AmazonS3Client s3Client;
  private TransferManager manager ;
  
  public S3Client(String regionName) {
    this(Region.getRegion(Regions.fromName(regionName)));
  }
  
  public S3Client() {
    this(Region.getRegion(Regions.DEFAULT_REGION));
  }

  public S3Client(Region region) {
    ClientConfiguration conf = new ClientConfiguration() ;
    conf.setMaxConnections(100);
    conf.setMaxErrorRetry(10);
    s3Client = new AmazonS3Client(conf);
    s3Client.setRegion(region);
    manager = new TransferManager(s3Client);
  }
  
  @PreDestroy
  public void onDestroy() {
    s3Client.shutdown();
  }

  public AmazonS3Client getAmazonS3Client() { return this.s3Client; }

  public Bucket createBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
    return s3Client.createBucket(bucketName);
  }

  public boolean hasBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
    return s3Client.doesBucketExist(bucketName);
  }

  public void deleteBucket(String bucketName, boolean recursive) throws AmazonClientException, AmazonServiceException {
    if (recursive) {
      deleteKeyWithPrefix(bucketName, "");
    }
    DeleteBucketRequest request = new DeleteBucketRequest(bucketName);
    s3Client.deleteBucket(request);
  }

  public void create(String bucketName, String key, byte[] data, String mimeType) throws AmazonServiceException {
    InputStream is = new ByteArrayInputStream(data);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(mimeType);
    s3Client.putObject(new PutObjectRequest(bucketName, key, is, metadata));
  }

  public boolean hasKey(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
    try {
      s3Client.getObjectMetadata(bucketName, key);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) return false;
      throw e;
    }
    return true;
  }

  public ObjectMetadata getObjectMetadata(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
    try {
      return s3Client.getObjectMetadata(bucketName, key);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) return null;
      throw e;
    }
  }

  public void createObject(String bucketName, String key, byte[] data, ObjectMetadata metadata) {
    InputStream is = new ByteArrayInputStream(data);
    createObject(bucketName, key, is, metadata);
  }

  public void createObject(String bucketName, String key, InputStream is, ObjectMetadata metadata) {
    s3Client.putObject(new PutObjectRequest(bucketName, key, is, metadata));
    metadata.addUserMetadata("transaction", "test");
  }

  public void updateObjectMetadata(String bucketName, String key, ObjectMetadata metadata) {
    CopyObjectRequest request = new CopyObjectRequest(bucketName, key, bucketName, key).withNewObjectMetadata(metadata);
    s3Client.copyObject(request);
  }

  public S3Object getObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
    return s3Client.getObject(bucketName, key);
  }

  public S3Folder createS3Folder(String bucketName, String folderPath) throws AmazonClientException, AmazonServiceException {
    create(bucketName, folderPath, new byte[0], "s3system/folder");
    return new S3Folder(this, bucketName, folderPath);
  }

  public void deleteS3Folder(String bucketName, String folderPath) {
    deleteKeyWithPrefix(bucketName, folderPath);
  }

  public S3Folder getS3Folder(String bucketName, String folderPath) throws AmazonClientException, AmazonServiceException {
    if (getObjectMetadata(bucketName, folderPath) == null) {
      throw new AmazonServiceException("Folder " + folderPath + " does not exist");
    }
    return new S3Folder(this, bucketName, folderPath);
  }

  public List<S3Folder> getRootFolders(String bucket) {
    List<S3Folder> folders = new ArrayList<S3Folder>();

    ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withDelimiter("/");
    ObjectListing objectListing = getAmazonS3Client().listObjects(request);
    for (String folderName : objectListing.getCommonPrefixes()) {
      folderName = folderName.substring(0, folderName.indexOf("/"));
      S3Folder folder = new S3Folder(this, bucket, folderName);
      folders.add(folder);
    }
    return folders;
  }

  public List<String> listKeyWithPrefix(String bucketName, String keyPrefix) {
    List<String> keyHolder = new ArrayList<>();
    ListObjectsRequest request = new ListObjectsRequest();
    request.
      withBucketName(bucketName).
      withPrefix(keyPrefix);
    String marker = "first";
    while(marker != null) {
      ObjectListing oListing = s3Client.listObjects(request);
      for (S3ObjectSummary sel : oListing.getObjectSummaries()) {
        keyHolder.add(sel.getKey());
      }
      marker = oListing.getNextMarker();
      request.setMarker(marker);
    }
    return keyHolder;
  }
  
  public List<String> listKeyChildren(String bucketName, String key, String delimiter) {
    List<String> keyHolder = new ArrayList<>();
    ListObjectsRequest request = new ListObjectsRequest();
    request.
      withBucketName(bucketName).
      withPrefix(key + delimiter).withDelimiter(delimiter);
    String marker = "first";
    while(marker != null) {
      ObjectListing oListing = s3Client.listObjects(request);
      for (S3ObjectSummary sel : oListing.getObjectSummaries()) {
        keyHolder.add(sel.getKey());
      }
      marker = oListing.getNextMarker();
      request.setMarker(marker);
    }
    return keyHolder;
  }
  
  public void deleteKeyWithPrefix(String bucketName, String prefix) {
    List<String> keyHolder = listKeyWithPrefix(bucketName, prefix);
    if(keyHolder.size() == 0) return;
    String[] keys = new String[keyHolder.size()];
    keys = keyHolder.toArray(keys);
    DeleteObjectsRequest delReq = new DeleteObjectsRequest(bucketName);
    delReq.withKeys(keys);
    s3Client.deleteObjects(delReq);
  }
  
  static public class Args {
    @Parameter(names = "--bucket-name", description = "bucket name")
    public String bucketName ;
    
    @Parameter(names = "--folder", description = "folder path")
    public String folder ;
    
    @Parameter(names = "--command", description = "command")
    public String command ;
  }
  
  static public void main(String[] args) throws Exception {
    S3Client s3Client = new S3Client();
    
  }
}