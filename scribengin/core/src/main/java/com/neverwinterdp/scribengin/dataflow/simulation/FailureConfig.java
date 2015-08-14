package com.neverwinterdp.scribengin.dataflow.simulation;

import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;

public class FailureConfig {
  static public enum FailurePoint { Before, After, Random }
  
  private String       activityType;
  private String       activityStepType;
  private FailurePoint failurePoint;
  private long         delay = 0;
  
  public FailureConfig() {} 
  
  public FailureConfig(String activity, String step, FailurePoint failurePoint, long delay) {
    this.activityType     = activity;
    this.activityStepType = step;
    this.failurePoint     = failurePoint;
  }
  
  public String getActivityType() { return activityType; }
  public void setActivityType(String activityType) {
    this.activityType = activityType;
  }
  
  public String getActivityStepType() { return activityStepType; }
  public void setActivityStepType(String activityStepType) {
    this.activityStepType = activityStepType;
  }
  
  public FailurePoint getFailurePoint() { return failurePoint; }
  public void setFailurePoint(FailurePoint failurePoint) {
    this.failurePoint = failurePoint;
  }
  
  public long getDelay() { return delay; }
  public void setDelay(long delay) { this.delay = delay; }

  public boolean matches(Activity activity) {
    if(activityType == null) return false;  
    return activity.getType().equals(activityType) ;
  }
  
  public boolean matches(ActivityStep step) {
    if(activityStepType == null) return true ;
    return step.getType().equals(activityStepType) ;
  }
  
  public boolean matches(FailurePoint failurePoint) {
    if(failurePoint == null) return true ;
    return failurePoint == this.failurePoint ;
  }
  
  public boolean matches(Activity activity, ActivityStep step, FailurePoint failurePoint) {
    if(activityType != null && !activity.getType().equals(activityType)) return false;  
    if(activityStepType != null && !step.getType().equals(activityStepType)) return false ;
    return failurePoint == this.failurePoint ;
  }
}
