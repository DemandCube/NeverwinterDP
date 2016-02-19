package com.neverwinterdp.analytics.web.generator;

import com.neverwinterdp.analytics.web.BrowserInfo;

public class Browsers {
  static public BrowserInfo[] getAll() {
    BrowserInfo[] all = {
      createMacbookAirBrowser(), createIphone6SBrowser(), createXRobotBrowser()
    } ;
    return all;
  }
  
  static public BrowserInfo createMacbookAirBrowser() {
    BrowserInfo browser = new BrowserInfo();
    browser.setDeviceName("Macbook Air");
    browser.setDeviceFamily("macbook");
    browser.setOsName("OS X Yosemite");
    browser.setOsFamily("OS X");
    browser.setBrowserName("Google Chrome - Version 48.0.2564.109");
    browser.setBrowserFamily("Google Chrome");
    browser.setScreenWidth(1440);
    browser.setScreenHeight(900);
    browser.setWindowWidth(900);
    browser.setWindowHeight(750);
    return browser;
  }
  
  static public BrowserInfo createIphone6SBrowser() {
    BrowserInfo browser = new BrowserInfo();
    browser.setDeviceName("Iphone 6S");
    browser.setDeviceFamily("Iphone");
    browser.setOsName("IOS V9.0");
    browser.setOsFamily("IOS");
    browser.setBrowserName("Safari - Version ?");
    browser.setBrowserFamily("Safari");
    browser.setScreenWidth(1080);
    browser.setScreenHeight(1920);
    browser.setWindowWidth(1080);
    browser.setWindowHeight(1920);
    return browser;
  }
  
  static public BrowserInfo createXRobotBrowser() {
    BrowserInfo browser = new BrowserInfo();
    browser.setDeviceName("X-Robot - GoogleBot");
    browser.setDeviceFamily("X-Robot");
    browser.setOsName("Linux - Version ?");
    browser.setOsFamily("Linux");
    browser.setBrowserName("Googlebot Crawler - Version ?");
    browser.setBrowserFamily("Crawler");
    browser.setScreenWidth(1080);
    browser.setScreenHeight(1920);
    browser.setWindowWidth(1080);
    browser.setWindowHeight(1920);
    return browser;
  }
}
