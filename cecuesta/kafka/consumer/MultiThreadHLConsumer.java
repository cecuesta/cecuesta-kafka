package cecuesta.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class MultiThreadHLConsumer {
  private ExecutorService executor;
  private final ConsumerConnector consumer;
  private final String topic;

  public MultiThreadHLConsumer(String zookeeper, String grupo, String topic) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", grupo);
    props.put("zookeeper.session.timeout.ms", "500");
    props.put("zookeeper.sync.time.ms", "250");
    props.put("auto.commit.interval.ms", "1000");

    consumer = Consumer.createJavaConsumerConnector(
      new ConsumerConfig(props));
    this.topic = topic;
    }

  public void pruebaConsumer(int hilo_cont) {
    Map<String, Integer> topic_cont = new HashMap<String, Integer>();

    // Define un contador de hilo para cada topic:
    topic_cont.put(topic, new Integer(hilo_cont));

    // Cómo hacerlo con un único topic y varios hilos
    // La extensión a varios topics es trivial
    // 	  (teniendo en cuenta que topic_cont es un Map)
    // ---
    Map<String, List<KafkaStream<byte[], byte[]>>> muchosStreams =
      consumer.createMessageStreams(topic_cont);

    List<KafkaStream<byte[], byte[]>> variosStreams =
      muchosStreams.get(topic);

    // Lanzamos un pool de hilos
    executor = Executors.newFixedThreadPool(hilo_cont);

    // Consumo mensajes en distintos hilos
    int nHilo = 0;

    for (final KafkaStream stream: variosStreams) {
      ConsumerIterator<byte[], byte[]> iteraConsumer = stream.iterator();
      nHilo++;
      while (iteraConsumer.hasNext()) 
	System.out.println("Mensaje del hilo: " + nHilo +
	  ": " + new String(iteraConsumer.next().message()));
      } 

      if (consumer != null) consumer.shutdown();
      if (executor != null) executor.shutdown();
    }

    public static void main(String[] args) {
      String topic = args[0];
      int hilo_cont = Integer.parseInt(args[1]);

      MultiThreadHLConsumer multiHLConsumer = 
        new MultiThreadHLConsumer("localhost:2181", "cecuesta_21102015", topic);

      multiHLConsumer.pruebaConsumer(hilo_cont);
      }
}

