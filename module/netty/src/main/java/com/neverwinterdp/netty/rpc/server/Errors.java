package com.neverwinterdp.netty.rpc.server;

import com.neverwinterdp.netty.rpc.protocol.Error;
import com.neverwinterdp.util.ExceptionUtil;

public class Errors {
  static public Error serviceNotFound(Throwable error) {
    return error(1, "Service Not Found", error) ;
  }
  
  static public Error methodNotFound(Throwable error) {
    return error(2, "Method Not Found", error) ;
  }
  
  static public Error methodInvalidParam(Throwable error) {
    return error(3, "Method Invalid Param", error) ;
  }
  
  static public Error methodInvokeError(Throwable error) {
    return error(3, "Method Invoke Error", error) ;
  }
  
  static Error error(int code, String message, Throwable error) {
    Error.Builder errorB = Error.newBuilder() ;
    errorB.setErrorCode(code) ;
    errorB.setMessage(message) ;
    errorB.setStacktrace(ExceptionUtil.getStackTrace(error)) ;
    return errorB.build() ;
  }
  
}
