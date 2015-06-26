package com.neverwinterdp.module;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class Hello implements HelloMBean {
  @Inject @Named("hello.hello")
  private String hello ;
  
  @Inject
  public void registerMBean(MBeanServer server) throws Exception {
    ObjectName oname = new ObjectName("com.neverwinterdp:type=HelloMBean,name=scribengin.Hello");
    server.registerMBean(this, oname);
    System.out.println("Init mbean...................");
  }
  
  @Override
  public void sayHello() {
    System.out.println("Say hello: " + hello);
  }
}