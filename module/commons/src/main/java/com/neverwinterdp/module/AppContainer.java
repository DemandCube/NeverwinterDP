package com.neverwinterdp.module;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.name.Names;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.ModuleRegistration.InstallStatus;
import com.neverwinterdp.module.ModuleRegistration.RunningStatus;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;
/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class AppContainer {
  private Injector                        appContainer;

  private RuntimeEnv                      runtimeEnv;

  private MetricRegistry                  metricRegistry;

  private LoggerFactory                   loggerFactory;
  private Logger                          logger;

  private Map<String, ModuleRegistration> availableModules = new ConcurrentHashMap<String, ModuleRegistration>();
  private Map<String, ServiceModuleContainer>   installedModules = new ConcurrentHashMap<String, ServiceModuleContainer>();

  public AppContainer(Map<String, String> properties) {
    Module[] modules = {
      new CloseableModule(), new Jsr250Module(), new MycilaJmxModuleExt("test-domain"), new AppModule(properties)
    };
    appContainer = Guice.createInjector(Stage.PRODUCTION, modules);
    runtimeEnv = appContainer.getInstance(RuntimeEnv.class);
    metricRegistry = appContainer.getInstance(MetricRegistry.class);
    loggerFactory = appContainer.getInstance(LoggerFactory.class);
    logger = loggerFactory.getLogger(getClass());
  }
  
  public void onInit() {
    logger.info("Start onInit()") ;
    ModuleRegistration.loadByAnnotation(availableModules, "com.neverwinterdp.module");
    ArrayList<String> moduleNames = new ArrayList<String>() ;
    for(ModuleRegistration sel : availableModules.values()) {
      if(sel.isAutoInstall()) moduleNames.add(sel.getModuleName()) ;
    }
    install(null, moduleNames.toArray(new String[moduleNames.size()])) ;
    logger.info("Finish onInit()");
  }
  
  public void onDestroy() {
    logger.info("Start onDestroy()");
    for(ServiceModuleContainer sel : installedModules.values()) {
      sel.uninstall(appContainer);
    }
    logger.info("Finish onDestroy()");
  }

  public ServiceModuleContainer getModule(String name) { return installedModules.get(name); }
  
  public ModuleRegistration[] install(Map<String, String> properties, String ...moduleNames)  {
    logger.info("Start install(String ... moduleNames)");
    List<ModuleRegistration> moduleStatusHolder = new ArrayList<> () ;
    for(int i = 0; i < moduleNames.length; i++) {
      if(installedModules.containsKey(moduleNames[i])) {
        logger.info("Module " + moduleNames[i] + " is already installed");
        continue ;
      }
      
      ModuleRegistration mreg = availableModules.get(moduleNames[i]) ;
      if(mreg == null) {
        logger.info("Module " + moduleNames[i] + " is not available");
        continue ;
      }
      Timer.Context timeCtx = metricRegistry.timer("server", "install", moduleNames[i]).time() ;
      try {
        Class<ServiceModule> clazz = (Class<ServiceModule>) Class.forName(mreg.getConfigureClass());
        ServiceModule module = clazz.newInstance() ;
        module.init(properties, runtimeEnv);
        ServiceModuleContainer scontainer = appContainer.getInstance(ServiceModuleContainer.class) ;
        scontainer.init(mreg, module, loggerFactory);
        scontainer.install(appContainer);
        installedModules.put(mreg.getModuleName(), scontainer) ;
        mreg.setInstallStatus(InstallStatus.INSTALLED);
        moduleStatusHolder.add(mreg) ;
      } catch(Exception ex) {
        logger.error("Cannot install the module " + moduleNames[i], ex);
      }
      long duration = timeCtx.stop() ;
      logger.info("Install module " + moduleNames[i] + " in " + TimeUnit.NANOSECONDS.toMillis(duration) + "ms");
    }
    logger.info("Finish install(String ... moduleNames)");
    return moduleStatusHolder.toArray(new ModuleRegistration[moduleStatusHolder.size()]) ;
  }
  
  public ModuleRegistration[] uninstall(String ... moduleNames) throws Exception {
    logger.info("Start  uninstall(String ... moduleNames)");
    List<ModuleRegistration> holder = new ArrayList<ModuleRegistration>() ;
    for(int i = 0; i < moduleNames.length; i++) {
      ServiceModuleContainer scontainer = installedModules.get(moduleNames[i]) ;
      if(scontainer != null) {
        Timer.Context timeCtx = metricRegistry.timer("server", "uninstall", moduleNames[i]).time() ;
        installedModules.remove(moduleNames[i]) ;
        ModuleRegistration mstatus = scontainer.getModuleStatus() ;
        scontainer.stop(); 
        scontainer.uninstall(appContainer);
        mstatus.setInstallStatus(InstallStatus.AVAILABLE);
        mstatus.setRunningStatus(RunningStatus.UNINSTALLED);
        holder.add(mstatus) ;
        long duration = timeCtx.stop() ;
        duration = TimeUnit.NANOSECONDS.toMillis(duration) ;
        logger.info("Uninstall module " + moduleNames[i] + " in " + duration + "ms");
      } else {
        logger.warn("Cannot find the module " + moduleNames[i] + " to uninstall");
      }
    }
    logger.info("Finish uninstall(String ... moduleNames)");
    return holder.toArray(new ModuleRegistration[holder.size()]) ;
  }
  
  public void start() {
    logger.info("Start start()");
    for(ServiceModuleContainer container : installedModules.values()) {
      if(container.getModuleStatus().isAutostart()) {
        container.start();
      }
    }
    logger.info("Finish start()");
  }

  public ModuleRegistration[] start(String ...moduleNames)  {
    logger.info("Start start(String ... moduleNames)");
    List<ModuleRegistration> moduleStatusHolder = new ArrayList<> () ;
    for(int i = 0; i < moduleNames.length; i++) {
      ServiceModuleContainer scontainer = installedModules.get(moduleNames[i]) ;
      if(scontainer == null) {
        logger.info("Module " + moduleNames[i] + " is not installed");
        continue ;
      }
      scontainer.start() ; 
      ModuleRegistration mstatus = scontainer.getModuleStatus() ;
      moduleStatusHolder.add(mstatus) ;
    }
    logger.info("Finish start(String ... moduleNames)");
    return moduleStatusHolder.toArray(new ModuleRegistration[moduleStatusHolder.size()]) ;
  }
  
  public void stop() {
    logger.info("Start stop()");
    for(ServiceModuleContainer container : installedModules.values()) {
      container.stop() ;
    }
    logger.info("Finish stop()");
  }

  public ModuleRegistration[] stop(String ...moduleNames)  {
    logger.info("Start stop(String ... moduleNames)");
    List<ModuleRegistration> moduleStatusHolder = new ArrayList<> () ;
    for(int i = 0; i < moduleNames.length; i++) {
      ServiceModuleContainer scontainer = installedModules.get(moduleNames[i]) ;
      if(scontainer == null) {
        logger.info("Module " + moduleNames[i] + " is not installed");
        continue ;
      }
      scontainer.stop() ; 
      ModuleRegistration mstatus = scontainer.getModuleStatus() ;
      moduleStatusHolder.add(mstatus) ;
    }
    logger.info("Finish stop(String ... moduleNames)");
    return moduleStatusHolder.toArray(new ModuleRegistration[moduleStatusHolder.size()]) ;
  }
  
  public <T> T getInstance(String module, Class<T> type) {
    ServiceModuleContainer container = installedModules.get(module) ;
    if(container != null) return container.getInstance(type);
    return null ;
  }
  
  public ModuleRegistration[] getAvailableModules() {
    return this.availableModules.values().toArray(new ModuleRegistration[availableModules.size()]) ;
  }
  
  public ModuleRegistration[] getInstalledModules() {
    ModuleRegistration[] array = new ModuleRegistration[installedModules.size()] ;
    int idx = 0 ;
    for(ServiceModuleContainer sel : installedModules.values()) {
      array[idx++] = sel.getModuleStatus() ;
    }
    return array ;
  }
  
  static public class AppModule extends AbstractModule {
    private Map<String, String> properties ;
    
    public AppModule() {
      properties = new HashMap<String, String>() ;
    }
    
    public AppModule(Map<String, String> properties) {
      this.properties = properties ;
    }
    
    @Override
    protected void configure() {
      Names.bindProperties(binder(), properties) ;
      
      RuntimeEnv runtimeEnv = new RuntimeEnv("server", "vm", "build/vm") ;
      bind(RuntimeEnv.class).toInstance(runtimeEnv);
      
      MetricRegistry metricRegistry = new MetricRegistry("server") ;
      bind(MetricRegistry.class).toInstance(metricRegistry);
      
      LoggerFactory loggerFactory = new LoggerFactory("[NeverwinterDP]") ;
      bind(LoggerFactory.class).toInstance(loggerFactory);
    }
  }
}
