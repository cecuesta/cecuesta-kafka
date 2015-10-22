package cecuesta.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleHLConsumer {
  private final ConsumerConnector consumer;
  private final String topic;

  public SimpleHLConsumer(String zookeeper, String grupo, String topic) {
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

  public void pruebaConsumer() {
    Map<String, Integer> topic_cont = new HashMap<String, Integer>();
    // Define un único hilo por topic:
    topic_cont.put(topic, new Integer(1));

    Map<String, List<KafkaStream<byte[], byte[]>>> muchosStreams =
      consumer.createMessageStreams(topic_cont);

    List<KafkaStream<byte[], byte[]>> variosStreams =
      muchosStreams.get(topic);

    for (final KafkaStream stream: variosStreams) {
      ConsumerIterator<byte[], byte[]> iteraConsumer = stream.iterator();
      while (iteraConsumer.hasNext()) 
	System.out.println("Mensaje de topic único: " + 
	  new String(iteraConsumer.next().message()));
      }
      if (consumer != null) consumer.shutdown();
    }

    public static void main(String[] args) {
      String topic = args[0];

      SimpleHLConsumer simpleHLConsumer = 
        new SimpleHLConsumer("localhost:2181", "cecuesta_21102015", topic);

      simpleHLConsumer.pruebaConsumer();
      }
}

