package net.pincette.rs.streams;

import java.util.Map;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;

/**
 * The interface for a consumer of messages from a topic.
 *
 * @param <K> the message key type.
 * @param <V> the message value type.
 * @param <T> the type the consumer uses for the consumed messages.
 * @author Werner Donné
 */
public interface TopicSource<K, V, T> {
  /**
   * Returns a processor that converts messages from the type the consumer uses to generic messages.
   *
   * @param topic the topic that will be consumed.
   * @return The processor.
   */
  Processor<T, Message<K, V>> connect(final String topic);

  /**
   * Returns the publishers for all the topics this consumer handles.
   *
   * @return The publishers.
   */
  Map<String, Publisher<T>> publishers();

  /**
   * Starts the topic source. This method blocks until either the source is signalled to stop or no
   * more streams are consuming anything.
   */
  void start();

  /** Signals the topic source to stop. This will unblock the <code>start</code> method. */
  void stop();
}
