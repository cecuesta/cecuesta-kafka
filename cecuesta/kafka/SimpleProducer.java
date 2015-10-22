package cecuesta.kafka;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
  private static Producer<Integer, String> producer;
  private final Properties props= new Properties();

  public SimpleProducer() {
    props.put("metadata.broker.list", "localhost:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    producer = new Producer<Integer, String>(new ProducerConfig(props));
  }

  public static void main(String[] args) {
    SimpleProducer sp = new SimpleProducer();
    String topic = (String) args[0];
    String mensaje = (String) args[1];
    KeyedMessage<Integer, String> datos = new KeyedMessage<Integer,
      String>(topic, mensaje);
    producer.send(datos);
    producer.close();
  }
}

