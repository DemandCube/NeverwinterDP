package com.neverwinterdp.kafka.producer;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class DefaultKafkaWriter extends AbstractKafkaWriter {
  final static public  Charset UTF8 = Charset.forName("UTF-8") ;
  
  private Properties kafkaProperties;
  private KafkaProducer<byte[], byte[]> producer;

  public DefaultKafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls);
  }

  public DefaultKafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    super(name);
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, name);
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerUrls);
    kafkaProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    kafkaProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
    
    kafkaProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    kafkaProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    kafkaProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    kafkaProps.setProperty(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true");
    
    kafkaProps.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
    kafkaProps.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "10");
    
    kafkaProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    
    this.kafkaProperties = kafkaProps;
    if (props != null) {
      kafkaProps.putAll(props);
    }
    this.kafkaProperties = kafkaProps;
    reconnect();
  }

  public void reconnect() {
    if (producer != null) producer.close();
    producer = new KafkaProducer<byte[], byte[]>(kafkaProperties);
  }
  
  public void send(String topic, int partition, byte[] key, byte[] data, Callback callback, long timeout) throws Exception {
    ProducerRecord<byte[], byte[]> record = null;
    if(partition >= 0) record = new ProducerRecord<byte[], byte[]>(topic, partition, key, data);
    else record = new ProducerRecord<byte[], byte[]>(topic, key, data);
    producer.send(record, callback);
  }
  
  public void close() { producer.close(); }
}