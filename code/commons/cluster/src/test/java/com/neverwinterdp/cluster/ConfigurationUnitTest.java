package com.neverwinterdp.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.neverwinterdp.server.config.Configuration;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.util.JSONSerializer;

public class ConfigurationUnitTest {
  @Test
  public void testConfiguration() throws Exception {
    String jsonConfig = 
      IOUtil.getFileContentAsString("src/main/resources/server-default-config.json", "UTF-8") ;
    Configuration conf = JSONSerializer.INSTANCE.fromString(jsonConfig, Configuration.class) ;
    assertNotNull(conf) ;
    assertEquals(5700, conf.getServer().getListenPort()) ;
    assertEquals(1.0, conf.getServer().getVersion(), 0) ;
  }
}
