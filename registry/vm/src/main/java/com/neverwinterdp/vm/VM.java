package com.neverwinterdp.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.mycila.guice.ext.closeable.CloseableInjector;
import com.neverwinterdp.module.AppContainer;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.module.ServiceModuleContainer;
import com.neverwinterdp.module.VMModule;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.util.io.NetworkUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMApp.TerminateEvent;
import com.neverwinterdp.vm.command.VMCommandWatcher;
import com.neverwinterdp.vm.service.VMService;

public class VM {
  static private Map<String, VM> vms = new ConcurrentHashMap<String, VM>() ;
  
  private Logger                 logger  ;

  private VMDescriptor           vmDescriptor;
  private VMStatus               vmStatus = VMStatus.INIT;

  private AppContainer           appContainer;
  private ServiceModuleContainer vmModuleContainer;
  
  private VMApplicationRunner    vmApplicationRunner;
  
  public VM(VMConfig vmConfig) throws Exception {
    if(vmConfig.isSelfRegistration()) {
      vmDescriptor = new VMDescriptor(vmConfig);
      initContainer(vmDescriptor, vmConfig);
      logger.info("Create VM with VMConfig:");
      logger.info(JSONSerializer.INSTANCE.toString(vmConfig));
      logger.info("Start self registration with the registry");
      
      Registry registry = vmModuleContainer.getInstance(Registry.class);
      VMService.register(registry, vmDescriptor);
      logger.info("Finish self registration with the registry");
    } else {
      vmDescriptor = initContainer(null, vmConfig);
    }
    init();
  }
  
  public Logger getLogger() { return logger; }
  
  public AppContainer getAppContainer() { return this.appContainer ; }
  
  public ServiceModuleContainer getVMModuleServiceContainer() { return this.vmModuleContainer ; }
  
  public LoggerFactory getLoggerFactory() { return appContainer.getLoggerFactory(); }
  
  public RuntimeEnv getRuntimeEnv() { return appContainer.getRuntimeEnv(); }
  
  private VMDescriptor initContainer(VMDescriptor vmDescriptor, final VMConfig vmConfig) throws Exception {
    final String vmDescriptorPath = VMService.ALL_PATH + "/" + vmConfig.getVmId();
    final RegistryConfig rConfig = vmConfig.getRegistryConfig();
    final Registry registry = rConfig.newInstance().connect();
    
    if(vmDescriptor == null) {
      vmDescriptor = registry.getDataAs(vmDescriptorPath, VMDescriptor.class);
    }
    final VMDescriptor finalVMDescriptor  = vmDescriptor;
    Map<String, String> props = vmConfig.getProperties();
    props.put("vm.registry.allocated.path", vmDescriptorPath);
    String hostname = NetworkUtil.getHostname();
    String localAppHome = vmConfig.getLocalAppHome();
    String localAppDataDir = localAppHome + "/data";
    AppModule appModule = new AppModule(hostname, vmConfig.getVmId(), localAppHome, localAppDataDir, props) {
      @Override
      protected void configure(Map<String, String> properties) {
        super.configure(properties);
        try {
          bindInstance(VMConfig.class, vmConfig);
          bindInstance(VMDescriptor.class, finalVMDescriptor);
          
          bindInstance(Registry.class, registry);
        } catch(Exception e) {
          e.printStackTrace();
        }
      }
    };
    appContainer = new AppContainer(appModule);
    appContainer.onInit();
    logger = appContainer.getLoggerFactory().getLogger(getClass());
    appContainer.install(new HashMap<String, String>(), VMModule.NAME) ;
    
    vmModuleContainer = appContainer.getModule("VMModule");
    return finalVMDescriptor;
  }
  
  public VMStatus getVMStatus() { return this.vmStatus ; }

  public VMDescriptor getDescriptor() { return vmDescriptor; }
  
  public VMApp getVMApplication() {
    if(vmApplicationRunner == null) return null;
    return vmApplicationRunner.vmApplication;
  }
  
  public VMRegistry getVMRegistry() { return appContainer.getInstance(VMRegistry.class) ; }

  public void setVMStatus(VMStatus status) throws RegistryException {
    vmStatus = status;
    getVMRegistry().updateStatus(status);
  }
  
  
  public void init() throws RegistryException {
    logger.info("Start init(...)");
    VMRegistry vmRegistry = getVMRegistry();
    setVMStatus(VMStatus.INIT);
    vmRegistry.addCommandWatcher(new VMCommandWatcher(this));
    vmRegistry.createHeartbeat();
    logger.info("Finish init(...)");
  }
  
  public void run() throws Exception {
    logger.info("Start run()");
    if(vmApplicationRunner != null) {
      throw new Exception("VM Application is already started");
    }
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    Class<VMApp> vmAppType = (Class<VMApp>)Class.forName(vmConfig.getVmApplication()) ;
    VMApp vmApp = vmAppType.newInstance();
    vmApp.setVM(this);
    setVMStatus(VMStatus.RUNNING);
    String threadName = "VM-" + vmApp.getVM().getDescriptor().getId();
    vmApplicationRunner = new VMApplicationRunner(threadName, vmApp, vmConfig.getProperties()) ;
    vmApplicationRunner.start();
    logger.info("Finish run()");
  }
  
  public void shutdown() throws Exception {
    terminate(TerminateEvent.Shutdown, 1000);
  }
  
  public void terminate(final TerminateEvent event, final long delay) throws Exception {
    if(vmApplicationRunner == null || !vmApplicationRunner.isAlive()) return;
    Thread thread = new Thread() {
      public void run() {
        try {
          if(delay > 0) {
            Thread.sleep(delay);
          }
          vmApplicationRunner.vmApplication.terminate(event);
          if(!vmApplicationRunner.vmApplication.isWaittingForTerminate()) {
            vmApplicationRunner.interrupt();
          }
        } catch (InterruptedException e) {
        }
      }
    };
    thread.start();
  }

  synchronized public void notifyComplete() {
    notifyAll();
  }
  
  synchronized public void waitForComplete() throws InterruptedException {
    long start = System.currentTimeMillis() ;
    logger.info("Start waitForComplete()");
    wait();
    logger.info("Finish waitForComplete() in " + (System.currentTimeMillis() - start) + "ms");
  }
  
  public class VMApplicationRunner extends Thread {
    VMApp vmApplication;
    Map<String, String> properties;
    
    public VMApplicationRunner(String threadName, VMApp vmApplication, Map<String, String> props) {
      super(new ThreadGroup(threadName), threadName);
      this.vmApplication = vmApplication;
      this.properties = props;
    }
    
    public void run() {
      try {
        vmApplication.run();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (Exception e) {
        logger.error("Error in vm application", e);
      } finally {
        try {
          setVMStatus(VMStatus.TERMINATED);
          appContainer.getInstance(CloseableInjector.class).close();
          appContainer.onDestroy();
          logger.info("Destroyed: " + vmDescriptor.getId() );
          System.err.println("Destroyed: " + vmDescriptor.getId() );
        } catch (RegistryException e) {
          System.err.println("Set terminated vm status for " + vmDescriptor.getId() );
          logger.error("Error in vm registry", e);
        }
        notifyComplete();
      }
    }
  }
  
  static public VM getVM(VMDescriptor descriptor) {
    return vms.get(descriptor.getId());
  }
  
  static public void trackVM(VM vm) {
    vms.put(vm.getDescriptor().getId(), vm);
  }
  
  static public VM run(VMConfig vmConfig) throws Exception {
    VM vm = new VM(vmConfig);
    vm.run();
    return vm;
  }
  
  static public void main(String[] args) throws Exception {
    long start = System.currentTimeMillis() ;
    System.out.println("VM: main(..) start");
    VMConfig vmConfig = new VMConfig(args);
    String vmDir = vmConfig.getLocalAppHome() ;
    System.setProperty("vm.app.dir", vmDir);
    System.setProperty("app.home", vmDir);
    
    Properties log4jProps = new Properties();
    log4jProps.load(IOUtil.loadRes(vmConfig.getLog4jConfigUrl()));
    System.setProperty("log4j.app.host", vmConfig.getVmId());
    String app = vmConfig.getVmApplication();
    app = app.substring(app.lastIndexOf('.') + 1);
    System.setProperty("log4j.app.name", app);
    LoggerFactory.log4jConfigure(log4jProps);
    
    VM vm = new VM(vmConfig);
    vm.run();
    vm.waitForComplete();
    System.out.println("VM: main(..) finish in " + (System.currentTimeMillis() - start) + "ms");
    System.exit(0);
  }
}