package com.neverwinterdp.netty.rpc.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;

public class ServiceRegistry {
  private static Logger log = LoggerFactory.getLogger(ServiceRegistry.class);

  private Map<String, ServiceDescriptor<?>> serviceNameMap = new HashMap<String, ServiceDescriptor<?>>();

  
  public Map<String, ServiceDescriptor<?>> getServices() {
    return Collections.unmodifiableMap(serviceNameMap);
  }

  public void clear() { serviceNameMap.clear(); }

  public String register(Service service) { 
    return register(service, true); 
  }

  public String register(Service service, boolean allowTimeout) {
    String serviceName = getServiceName(service.getDescriptorForType());
    if ( serviceNameMap.containsKey(serviceName) ) {
      throw new IllegalStateException("Duplicate serviceName "+ serviceName);
    }
    serviceNameMap.put(serviceName, new ServiceDescriptor.NonBlockingServiceDescriptor(allowTimeout, service));
    log.info("Registered NonBlocking " + serviceName +" allowTimeout="+(allowTimeout?"Y":"N"));

    return serviceName;
  }

  public String register(BlockingService blockingService) {
    return register(blockingService, true);
  }

  public String register(BlockingService service, boolean allowTimeout) {
    String serviceName = getServiceName(service.getDescriptorForType());
    if ( serviceNameMap.containsKey(serviceName) ) {
      throw new IllegalStateException("Duplicate serviceName "+ serviceName);
    }
    serviceNameMap.put(serviceName, new ServiceDescriptor.BlockingServiceDescriptor(allowTimeout, service));
    log.info("Registered Blocking " + serviceName + " allowTimeout="+(allowTimeout?"Y":"N"));

    return serviceName;
  }

  public void remove(Service serviceImpl) {
    String serviceName = getServiceName(serviceImpl.getDescriptorForType());
    if ( serviceNameMap.remove(serviceName) != null ) {
      log.info("Removed " + serviceName);
    }
  }

  public void remove(BlockingService serviceImpl) {
    String serviceName = getServiceName(serviceImpl.getDescriptorForType());
    if ( serviceNameMap.remove(serviceName) != null ) {
      log.info("Removed " + serviceName);
    }
  }
  
  public void remove(String serviceName) {
    if ( serviceNameMap.remove(serviceName) != null ) {
      log.info("Removed " + serviceName);
    }
  }

  public ServiceDescriptor<?> getService(String serviceName) {
    ServiceDescriptor<?> s = serviceNameMap.get(serviceName);
    if ( log.isDebugEnabled() ) {
      if ( s != null ) {
        log.debug("Resolved " + serviceName);
      } else {
        log.debug("Unable to resolve " + serviceName );
      }
    }
    return s;
  }

  //Issue 29: use FQN for services to avoid problems with duplicate service names in different pkgs.
  private String getServiceName(Descriptors.ServiceDescriptor descriptor ) {
    return descriptor.getFullName();
  }
}
