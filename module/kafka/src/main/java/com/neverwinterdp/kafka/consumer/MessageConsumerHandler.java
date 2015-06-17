package com.neverwinterdp.kafka.consumer ;

/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public interface MessageConsumerHandler {
  public void onMessage(String topic, byte[] key, byte[] message)  ;
}