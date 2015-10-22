package cecuesta.kafka;

import java.util.Properties;
import java.util.Random;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MultiBrokerProducer {
  private static Producer<Integer, String> producer;
  private final Properties props= new Properties();

  public MultiBrokerProducer() {
    props.put("metadata.broker.list", "localhost:9093, localhost:9094");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "cecuesta.kafka.SimplePartitioner");
    props.put("request.required.acks", "1");
    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<Integer, String>(config);
  }

  public static void main(String[] args) {
    MultiBrokerProducer mbp = new MultiBrokerProducer();
    Random rnd = new Random();
    String topic = (String) args[0];
    for (int contador=0; contador<10; contador++) {
      Integer clave = rnd.nextInt(255);
      String mensaje = "Mensaje para clave " + clave;
      KeyedMessage<Integer, String> datos = 
        new KeyedMessage<Integer, String> (topic, clave, mensaje);
      producer.send(datos);
    }
    producer.close();
  }
}

