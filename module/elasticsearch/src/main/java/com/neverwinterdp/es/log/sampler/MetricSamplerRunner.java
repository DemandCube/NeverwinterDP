package com.neverwinterdp.es.log.sampler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ObjectArrays;

public class MetricSamplerRunner {
  static public void main(String[] args) throws Exception {
    MetricSamplerConfig config = new MetricSamplerConfig();
    new JCommander(config, args);
    ExecutorService taskExecutor = Executors.newFixedThreadPool(config.numVM);
    
    for (int i = 0; i < config.numVM; i++) {
      String[] vmargs = { 
          "--vm-name", "vm-" + i};
      taskExecutor.execute(
          new MetricSamplerVMGenerator("-Xms128m -Xmx512m", MetricSampler.class.getName(), ObjectArrays.concat(args, vmargs, String.class)));
    }
    taskExecutor.shutdown();
    try {
      taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
