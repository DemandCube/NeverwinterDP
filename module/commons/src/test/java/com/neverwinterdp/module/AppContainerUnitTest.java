package com.neverwinterdp.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class AppContainerUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/console-log4j.properties");
  }

  @Test
  public void testAppContainer() throws Exception {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("hello.hello", "Hello Property") ;
    AppModule appModule = new AppModule("localhost", "vm-test", ".", "./build/app-data", props) {
      @Override
      protected void configure(Map<String, String> props) {
        super.configure(props);
      }
    };
    AppContainer appContainer = new AppContainer(appModule);
    appContainer.onInit();
    appContainer.install(new HashMap<String, String>(), "HelloModule");
    ServiceModuleContainer helloContainer = appContainer.getModule("HelloModule");
    Hello hello = helloContainer.getInstance(Hello.class);
    hello.sayHello();
    appContainer.onDestroy();
  }
  
  @Test
  public void testLoadModuleRegistration() {
    Map<String, ModuleRegistration> holder = new HashMap<String, ModuleRegistration>() ;
    ModuleRegistration.loadByAnnotation(holder, "com.neverwinterdp.module");
    
    ModuleRegistration hello = holder.get("HelloModule");
    assertNotNull(hello) ;
    assertEquals("HelloModule", hello.getModuleName()) ;
    assertEquals(HelloModule.class.getName(), hello.getConfigureClass()) ;
    assertFalse(hello.isAutoInstall()) ;
    assertFalse(hello.isAutostart()) ;
    
    ModuleRegistration helloDisable = holder.get("HelloModuleDisable");
    assertNotNull(helloDisable) ;
    assertEquals("HelloModuleDisable", helloDisable.getModuleName()) ;
    assertEquals(HelloModuleDisable.class.getName(), helloDisable.getConfigureClass()) ;
    assertFalse(helloDisable.isAutoInstall()) ;
    assertFalse(helloDisable.isAutostart()) ;
  }
}
