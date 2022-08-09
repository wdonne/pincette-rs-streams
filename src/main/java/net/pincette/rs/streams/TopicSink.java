package net.pincette.rs.streams;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;

/**
 * The interface for a producer that writes messages to topics.
 *
 * @param <K> the message key type.
 * @param <V> the message value type.
 * @param <T> the type the producer needs to write messages to a topic.
 * @author Werner Donn\u00e9
 */
public interface TopicSink<K, V, T> {
  /**
   * Returns a processor that converts the generic messages to type the producer needs.
   *
   * @param topic the topic that will be written to.
   * @return The processor.
   */
  Processor<Message<K, V>, T> connect(final String topic);

  /**
   * Returns the subscriber that represents the actual producer.
   *
   * @return The subscriber.
   */
  Subscriber<T> subscriber();
}
