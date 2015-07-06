package com.neverwinterdp.module;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@interface ModuleConfig {
  String name();
  boolean autoInstall() default false ;
  boolean autostart() default false;
}