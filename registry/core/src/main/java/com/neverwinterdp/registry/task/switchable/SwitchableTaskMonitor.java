package com.neverwinterdp.registry.task.switchable;

public interface SwitchableTaskMonitor<T> {
  public void onAssign(SwitchableTaskContext<T> context) ;
  public void onAvailable(SwitchableTaskContext<T> context) ;
  public void onFinish(SwitchableTaskContext<T> context) ;
  public void onFail(SwitchableTaskContext<T> context) ;
}
