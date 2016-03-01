package com.neverwinterdp.netty.http.client;

public class ClientInfo {
  public User        user;
  public WebPage     webpage;
  public Navigator   navigator;
  public Screen      screen;
  public Window      window;
  public GeoLocation geoLocation;

  public ClientInfo() {}
  
  public ClientInfo(boolean init) {
    user        = new User();
    webpage     = new WebPage();
    navigator   = new Navigator();
    screen      = new Screen();
    window      = new Window();
    geoLocation = new GeoLocation();
  }
  
  static public class User {
    public String userId;
    public String visitorId;
    public String ipAddress;
  }
  
  static public class WebPage {
    public String url;
    public String referralUrl;
  }
  
  static public class Navigator {
    public String   platform;
    public String   appCodeName;
    public String   appName;
    public String   appVersion;
    public boolean  cookieEnabled;
    public String   userAgent;
    public String   language;
    public String[] languages;
  }
  
  static public class Screen {
    public int width;
    public int height ;
  }
  
  static public class Window {
    public int width;
    public int height ;
  }
  
  static public class GeoLocation {
    public double latitude;
    public double longitude;
    public double accuracy;
  }
}