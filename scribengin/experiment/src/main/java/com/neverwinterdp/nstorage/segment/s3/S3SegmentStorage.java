package com.neverwinterdp.nstorage.segment.s3;

import com.neverwinterdp.nstorage.segment.SegmentStorage;
import com.neverwinterdp.nstorage.segment.SegmentStorageWriter;

public class S3SegmentStorage extends SegmentStorage {

  @Override
  public SegmentStorageWriter getWriter(String name) {
    return null;
  }
}
