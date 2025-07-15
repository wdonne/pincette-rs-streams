package net.pincette.rs.streams;

import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Util.onErrorProcessor;
import static net.pincette.util.Pair.pair;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import net.pincette.rs.Fanout;
import net.pincette.util.Pair;

/**
 * This is a small API to connect reactive streams from and to topics. Each instance uses its own
 * topic source and sink, which are shared by all the topics that are handled by the instance. It is
 * possible to create instances without either a source or a sink. This can be used to connect
 * instances with different message types.
 *
 * @param <K> the message key type.
 * @param <V> the message value type.
 * @param <T> the type the topic source uses to represent messages.
 * @param <U> the type the topic sink uses to represent messages.
 * @author Werner Donné
 */
public class Streams<K, V, T, U> {
  private static final Duration DEFAULT_GRACE_PERIOD = ofSeconds(3);
  private static final String FROM_ERROR = "No preceding call to from.";
  private static final String TOPIC_SINK_ERROR = "No topic sink.";
  private static final String TOPIC_SOURCE_ERROR = "No topic source.";

  private final Function<Set<String>, TopicSink<K, V, U>> topicSink;
  private final Function<Set<String>, TopicSource<K, V, T>> topicSource;
  private final List<Pair<String, Processor<Message<K, V>, Message<K, V>>>> topicConsumers =
      new ArrayList<>();
  private final List<Pair<String, Publisher<Message<K, V>>>> topicProducers = new ArrayList<>();
  private Duration gracePeriod = DEFAULT_GRACE_PERIOD;
  private Publisher<Message<K, V>> fromPublisher;
  private Consumer<Throwable> onError;
  private TopicSink<K, V, U> sink;
  private TopicSource<K, V, T> source;

  protected Streams(
      final Function<Set<String>, TopicSource<K, V, T>> topicSource,
      final Function<Set<String>, TopicSink<K, V, U>> topicSink) {
    this.topicSource = topicSource;
    this.topicSink = topicSink;
  }

  /**
   * Creates a streams instance with a topic source and sink function.
   *
   * @param topicSource the function that generates a topic source. It receives all the topics that
   *     will be consumed. It may be <code>null</code>, in which case the <code>from</code> methods
   *     can't be used.
   * @param topicSink the function that generates a topic sink. It receives all the topics that will
   *     be written to. It may be <code>null</code>, in which case the <code>to</code> methods can't
   *     be used.
   * @param <K> the message key type.
   * @param <V> the message value type.
   * @param <T> the type the topic source uses to represent messages.
   * @param <U> the type the topic sink uses to represent messages.
   * @return The streams instance.
   */
  public static <K, V, T, U> Streams<K, V, T, U> streams(
      final Function<Set<String>, TopicSource<K, V, T>> topicSource,
      final Function<Set<String>, TopicSink<K, V, U>> topicSink) {
    return new Streams<>(topicSource, topicSink);
  }

  private static <T> Set<String> topics(final List<Pair<String, T>> topics) {
    return topics.stream().map(pair -> pair.first).collect(toSet());
  }

  private static <K, V, T> Publisher<Message<K, V>> connect(
      final Publisher<T> publisher, final String topic, final TopicSource<K, V, T> source) {
    final Processor<T, Message<K, V>> processor = source.connect(topic);

    publisher.subscribe(processor);

    return processor;
  }

  private static <K, V, T> Publisher<T> connect(
      final Publisher<Message<K, V>> publisher, final String topic, final TopicSink<K, V, T> sink) {
    final Processor<Message<K, V>, T> processor = sink.connect(topic);

    publisher.subscribe(processor);

    return processor;
  }

  private Subscriber<Message<K, V>> connectTopicConsumers(final String topic) {
    final List<Subscriber<Message<K, V>>> consumers = topicConsumers(topic);

    return consumers.size() > 1 ? Fanout.of(consumers) : consumers.get(0);
  }

  /**
   * Attaches a reactive stream consumer to a topic.
   *
   * @param topic the given topic.
   * @param consumer the function that consumes the topic publisher.
   * @return The stream instance.
   */
  public Streams<K, V, T, U> consume(
      final String topic, final Consumer<Publisher<Message<K, V>>> consumer) {
    consumer.accept(from(topic));

    return this;
  }

  private TopicSink<K, V, U> createTopicSink() {
    if (topicSink != null) {
      final Set<String> topics = sinkTopics();

      if (!topics.isEmpty()) {
        final TopicSink<K, V, U> s = topicSink.apply(topics);

        topicProducers.stream()
            .map(pair -> connect(pair.second, pair.first, s))
            .forEach(publisher -> publisher.subscribe(sinkSubscriber(s)));

        return s;
      }
    }

    return null;
  }

  private TopicSource<K, V, T> createTopicSource() {
    if (topicSource != null) {
      final Set<String> topics = sourceTopics();

      if (!topics.isEmpty()) {
        final TopicSource<K, V, T> s = topicSource.apply(topics);

        s.publishers().forEach((k, v) -> connect(v, k, s).subscribe(connectTopicConsumers(k)));

        return s;
      }
    }

    return null;
  }

  /**
   * Returns the publisher for a topic that is consumed.
   *
   * @param topic the given topic.
   * @return The topic publisher.
   */
  public Publisher<Message<K, V>> from(final String topic) {
    if (topicSource == null) {
      throw new IllegalArgumentException(TOPIC_SOURCE_ERROR);
    }

    final Processor<Message<K, V>, Message<K, V>> publisher = passThrough();

    topicConsumers.add(pair(topic, publisher));

    return publisher;
  }

  /**
   * Attaches a reactive stream to a topic that can be fed into another topic.
   *
   * @param topic the given topic.
   * @param publisher the function that creates a new publisher from the topic publisher.
   * @return The stream instance.
   */
  public Streams<K, V, T, U> from(
      final String topic, final UnaryOperator<Publisher<Message<K, V>>> publisher) {
    if (topicSource == null) {
      throw new IllegalArgumentException(TOPIC_SOURCE_ERROR);
    }

    fromPublisher = publisher.apply(from(topic));

    return this;
  }

  /**
   * Attaches a reactive stream to a topic that can be fed into another topic.
   *
   * @param topic the given topic.
   * @param processor the given processor.
   * @return The stream instance.
   */
  public Streams<K, V, T, U> from(
      final String topic, final Processor<Message<K, V>, Message<K, V>> processor) {
    if (topicSource == null) {
      throw new IllegalArgumentException(TOPIC_SOURCE_ERROR);
    }

    fromPublisher = with(from(topic)).map(processor).get();

    return this;
  }

  /**
   * Registers an error signal consumer. If one was already registered it will be replaced.
   *
   * @param onError the function that consumes the error signals.
   * @return The stream instance.
   */
  public Streams<K, V, T, U> onError(final Consumer<Throwable> onError) {
    this.onError = onError;

    return this;
  }

  /**
   * Attaches a reactive stream to a publisher that was created with a previous call to either
   * <code>from</code> or <code>process</code>.
   *
   * @param publisher the function that creates a new publisher from the topic publisher.
   * @return The stream instance.
   */
  public Streams<K, V, T, U> process(final UnaryOperator<Publisher<Message<K, V>>> publisher) {
    if (fromPublisher == null) {
      throw new IllegalArgumentException(FROM_ERROR);
    }

    fromPublisher = publisher.apply(fromPublisher);

    return this;
  }

  /**
   * Attaches a reactive stream to a publisher that was created with a previous call to either
   * <code>from</code> or <code>process</code>.
   *
   * @param processor the given processor.
   * @return The stream instance.
   */
  public Streams<K, V, T, U> process(final Processor<Message<K, V>, Message<K, V>> processor) {
    if (fromPublisher == null) {
      throw new IllegalArgumentException(FROM_ERROR);
    }

    fromPublisher = with(fromPublisher).map(processor).get();

    return this;
  }

  private Subscriber<U> sinkSubscriber(final TopicSink<K, V, U> sink) {
    return onError != null
        ? net.pincette.rs.Util.subscribe(onErrorProcessor(onError::accept), sink.subscriber())
        : sink.subscriber();
  }

  /**
   * Returns all the topics that will be produced to.
   *
   * @return The topics.
   * @since 1.3.0
   */
  public Set<String> sinkTopics() {
    return topics(topicProducers);
  }

  /**
   * Returns all the topics that will be consumed from.
   *
   * @return The topics.
   * @since 1.3.0
   */
  public Set<String> sourceTopics() {
    return topics(topicConsumers);
  }

  /** Starts the streams instance. It blocks until the topic source finishes. */
  public void start() {
    sink = createTopicSink();
    source = createTopicSource();

    if (source != null) {
      source.start();
      stopSink(gracePeriod);
    }
  }

  /** Calls <code>stop</code> with a grace period of three seconds. */
  public void stop() {
    stop(DEFAULT_GRACE_PERIOD);
  }

  /**
   * Signals the topic source to stop. This will unblock the <code>start</code> method and stop the
   * sink after a grace period it didn't complete naturally.
   *
   * @param gracePeriod the grace period.
   */
  public void stop(final Duration gracePeriod) {
    this.gracePeriod =
        gracePeriod != null && !gracePeriod.isNegative() ? gracePeriod : DEFAULT_GRACE_PERIOD;

    if (source != null) {
      source.stop();
    } else {
      stopSink(gracePeriod);
    }
  }

  private void stopSink(final Duration gracePeriod) {
    if (sink != null) {
      sink.stop(gracePeriod);
    }
  }

  /**
   * Subscribes to a stream that was created by a preceding call to the <code>from</code> method.
   *
   * @param subscriber the given subscriber.
   * @return The streams instance.
   */
  public Streams<K, V, T, U> subscribe(final Subscriber<? super Message<K, V>> subscriber) {
    if (fromPublisher == null) {
      throw new IllegalArgumentException(FROM_ERROR);
    }

    fromPublisher.subscribe(subscriber);
    fromPublisher = null;

    return this;
  }

  /**
   * Connects a publisher to a topic.
   *
   * @param topic the topic that will be written to.
   * @param publisher the given publisher.
   * @return The streams instance.
   */
  public Streams<K, V, T, U> to(final String topic, final Publisher<Message<K, V>> publisher) {
    if (topicSink == null) {
      throw new IllegalArgumentException(TOPIC_SINK_ERROR);
    }

    topicProducers.add(pair(topic, publisher));

    return this;
  }

  /**
   * Connects the publisher that was created by the preceding call to the <code>from</code> method
   * to a topic.
   *
   * @param topic the topic that will be written to.
   * @return The streams instance.
   */
  public Streams<K, V, T, U> to(final String topic) {
    if (topicSink == null) {
      throw new IllegalArgumentException(TOPIC_SINK_ERROR);
    }

    if (fromPublisher == null) {
      throw new IllegalArgumentException(FROM_ERROR);
    }

    to(topic, fromPublisher);
    fromPublisher = null;

    return this;
  }

  private List<Subscriber<Message<K, V>>> topicConsumers(final String topic) {
    return topicConsumers.stream()
        .filter(pair -> pair.first.equals(topic))
        .map(pair -> pair.second)
        .collect(toList());
  }
}
