package com.neverwinterdp.scribengin.dataflow;

import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;

public class DataflowDescriptorUnitTest {
  @Test
  public void testConfig() throws Exception {
    String json = IOUtil.getFileContentAsString("src/test/resources/kafka-tracking-dataflow.json");
    DataflowDescriptor config = JSONSerializer.INSTANCE.fromString(json, DataflowDescriptor.class);
    Assert.assertNotNull(config);
  }
}
