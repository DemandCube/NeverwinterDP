package com.neverwinterdp.netty.http.client;

import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;

public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
  private int        bufferSize      = 1000;
  private AtomicLong requestCounter  = new AtomicLong();
  private AtomicLong responseCounter = new AtomicLong();
  
  private ResponseHandler responseHandler ;
  
  public HttpClientHandler(ResponseHandler handler) {
    this.responseHandler = handler ;
  }
  
  public int getBufferSize() { return this.bufferSize; }
  
  public long getRequestCount() { return requestCounter.get(); }
  
  public long getResponseCount() { return responseCounter.get(); }
  
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    try {
      responseCounter.incrementAndGet();
      if (!(msg instanceof HttpResponse)) return ;
      responseHandler.onResponse((HttpResponse) msg) ;
    } finally {
      notifyAvailableBuffer();
    }
  }
  
  synchronized public void waitForAvailableBuffer() throws InterruptedException {
    requestCounter.incrementAndGet();
    if(getRequestCount() - getResponseCount() > bufferSize) {
      wait(0);
    }
  }
  
  synchronized public void notifyAvailableBuffer() {
    notifyAll();
  }
  
  synchronized public void waitForAllResonse(long maxWaitTime) throws InterruptedException {
    long stopTime = System.currentTimeMillis() + maxWaitTime;
    while(getResponseCount() < getRequestCount()) {
      long waitTime = stopTime - System.currentTimeMillis();
      wait(waitTime);
    }
  }
}