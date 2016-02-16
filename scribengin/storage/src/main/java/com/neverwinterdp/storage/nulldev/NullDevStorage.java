package com.neverwinterdp.storage.nulldev;

import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.nulldev.sink.NullDevSink;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.source.Source;

public class NullDevStorage extends Storage {
  
  public NullDevStorage(StorageConfig storageDescriptor) {
    super(storageDescriptor);
  }
  
  @Override
  public void refresh() throws Exception {
  }

  public boolean exists() throws Exception {
    return true;
  }
  
  @Override
  public void drop() throws Exception {
  }

  @Override
  public void create() throws Exception {
  }

  @Override
  public Sink getSink() throws Exception {
    return new  NullDevSink(getStorageConfig());
  }

  @Override
  public Source getSource() throws Exception {
    throw new Exception("Not Supported");
  }
}
