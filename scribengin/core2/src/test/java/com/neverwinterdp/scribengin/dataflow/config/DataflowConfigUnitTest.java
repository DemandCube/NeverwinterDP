package com.neverwinterdp.scribengin.dataflow.config;

import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;

public class DataflowConfigUnitTest {
  @Test
  public void testConfig() throws Exception {
    String json = IOUtil.getFileContentAsString("src/test/resources/dataflow-config.json");
    DataflowConfig config = JSONSerializer.INSTANCE.fromString(json, DataflowConfig.class);
    Assert.assertNotNull(config);
  }
}
