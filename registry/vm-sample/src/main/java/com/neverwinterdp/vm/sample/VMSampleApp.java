package com.neverwinterdp.vm.sample;

import com.neverwinterdp.vm.VMApp;

public class VMSampleApp extends VMApp {
  @Override
  public void run() throws Exception {
    try {
      while(true) {
        System.out.println("Hello VM Sample App!!!");
        Thread.sleep(1000);
      }
    } catch(InterruptedException ex) {
    }
  }
}