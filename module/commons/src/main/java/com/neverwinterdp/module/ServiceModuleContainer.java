package com.neverwinterdp.module;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;

import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.neverwinterdp.module.ModuleRegistration.RunningStatus;
import com.neverwinterdp.util.log.LoggerFactory;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class ServiceModuleContainer {
  private Injector           container;
  private Logger             logger;
  private ModuleRegistration moduleStatus;
  private ServiceModule      module;

  public ServiceModule getModule() { return this.module ; }
  
  public ModuleRegistration getModuleStatus() { return this.moduleStatus ; }
  
  public void init(ModuleRegistration mstatus, ServiceModule module, LoggerFactory lfactory) {
    this.moduleStatus = mstatus ;
    this.module = module ;
    logger = lfactory.getLogger(module.getClass().getSimpleName()) ;
  }

  public void install(Injector appContainer) {
    uninstall(appContainer) ;
    logger.info("install(Injector parentContainer)");
    container = appContainer.createChildInjector(module) ;
    Map<Key<?>, Binding<?>> bindings = container.getBindings() ;
    for(Key<?> key : bindings.keySet()) {
      Object instance = container.getInstance(key) ;
      invokeAnnotatedMethod(instance, PostConstruct.class, new Object[] {}) ;
    }
    logger.info("Finish install(Injector parentContainer)");
  }
  
  public void uninstall(Injector appContainer) {
    if(container == null) return ;
    Map<Key<?>, Binding<?>> bindings = container.getBindings() ;
    for(Key<?> key : bindings.keySet()) {
      Object instance = container.getInstance(key) ;
      invokeAnnotatedMethod(instance, PreDestroy.class, new Object[] {}) ;
    }
    this.container = null ;
  }
  
  public <T> T getInstance(Class<T> type) {
    return container.getInstance(type);
  }
  
  public void start() {
    if(moduleStatus.getRunningStatus().equals(RunningStatus.START)) return ;
    moduleStatus.setRunningStatus(RunningStatus.START);
  }


  public void stop() {
    if(moduleStatus.getRunningStatus().equals(RunningStatus.STOP)) return ;
    moduleStatus.setRunningStatus(RunningStatus.STOP);
  }

  private <T extends Annotation> void invokeAnnotatedMethod(Object instance, Class<T> annotatedClass, Object[] args) {
    Method[] method = instance.getClass().getMethods() ;
    for(Method selMethod : method) {
      Annotation annotation = selMethod.getAnnotation(annotatedClass) ;
      if(annotation != null) {
        try {
          selMethod.invoke(instance, args) ;
        } catch (Exception e) {
          logger.warn("Cannot call " + selMethod.getName() + " for " + instance.getClass(), e);
        }
      }
    }
  }
}