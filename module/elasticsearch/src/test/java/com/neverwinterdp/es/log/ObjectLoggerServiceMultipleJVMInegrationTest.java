package com.neverwinterdp.es.log;

import org.junit.Test;

import com.neverwinterdp.es.log.sampler.MetricSamplerRunner;

public class ObjectLoggerServiceMultipleJVMInegrationTest {
  @Test
  public void testService() throws Exception {
    String[] args = {
      "--es-connect", "188.166.251.211:9300",
      "--num-vm", "10"
    };
    MetricSamplerRunner.main(args);
  }
}
