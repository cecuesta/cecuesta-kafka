package cecuesta.kafka;

import kafka.producer.Partitioner;

public class SimplePartitioner implements Partitioner {
  public int partition(Object clave, int nPartitions) {
    int partition = 0;
    int iKey = (Integer) clave;
    if (iKey > 0) {
      partition = iKey % nPartitions;
    }
    return partition;
  }
}
