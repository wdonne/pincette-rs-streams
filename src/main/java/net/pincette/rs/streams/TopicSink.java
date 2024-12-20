package net.pincette.rs.streams;

import java.time.Duration;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;

/**
 * The interface for a producer that writes messages to topics.
 *
 * @param <K> the message key type.
 * @param <V> the message value type.
 * @param <T> the type the producer needs to write messages to a topic.
 * @author Werner Donné
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
   * Stops the topic sink. This method blocks until all subscribers are stopped.
   *
   * @param gracePeriod the period after which the subscriptions that didn't complete naturally will
   *     be cancelled.
   */
  void stop(final Duration gracePeriod);

  /**
   * Returns a subscriber that represents the actual producer. Each call to this method should
   * create a new subscriber, because a subscriber can have only one subscription at the time.
   *
   * @return The subscriber.
   */
  Subscriber<T> subscriber();
}
