package com.neverwinterdp.es.log.sampler;

import com.beust.jcommander.Parameter;

public class MetricSamplerConfig {
  @Parameter(names = "--vm-name", description = "VM Name")
  public String vmName = "vm-1";
  
  @Parameter(names = "--es-connect", description = "Elasticsearch connect")
  public String esConnect = "127.0.0.1:9300";
}
