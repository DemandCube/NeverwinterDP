package com.neverwinterdp.wa.event;

import com.neverwinterdp.util.JSONSerializer;

public class BrowserInfo {
  private String deviceName;
  private String deviceFamily;
  
  private String osName ;
  private String osFamily;
  
  private String browserName;
  private String browserFamily;
  
  private int    windowWidth;
  private int    windowHeight;
  
  private int    screenWidth;
  private int    screenHeight;

  private String[] tags;

  public String getDeviceName() { return deviceName; }
  public void setDeviceName(String deviceName) { this.deviceName = deviceName; }

  public String getDeviceFamily() { return deviceFamily; }
  public void setDeviceFamily(String deviceFamily) { this.deviceFamily = deviceFamily; }

  public String getOsName() { return osName; }
  public void setOsName(String osName) { this.osName = osName; }

  public String getOsFamily() { return osFamily; }
  public void setOsFamily(String osFamily) { this.osFamily = osFamily; }

  public String getBrowserName() { return browserName; }
  public void setBrowserName(String browserName) { this.browserName = browserName; }

  public String getBrowserFamily() { return browserFamily; }
  public void setBrowserFamily(String browserFamily) { this.browserFamily = browserFamily; }

  public int getWindowWidth() { return windowWidth; }
  public void setWindowWidth(int windowWidth) { this.windowWidth = windowWidth; }

  public int getWindowHeight() { return windowHeight; }
  public void setWindowHeight(int windowHeight) { this.windowHeight = windowHeight; }

  public int getScreenWidth() { return screenWidth; }
  public void setScreenWidth(int screenWidth) { this.screenWidth = screenWidth; }

  public int getScreenHeight() { return screenHeight; }
  public void setScreenHeight(int screenHeight) { this.screenHeight = screenHeight; }

  public String[] getTags() { return tags; }
  public void setTags(String[] tags) { this.tags = tags; }
  
  public BrowserInfo clone() {
    byte[] data = JSONSerializer.INSTANCE.toBytes(this);
    return JSONSerializer.INSTANCE.fromBytes(data, BrowserInfo.class);
  }
}
