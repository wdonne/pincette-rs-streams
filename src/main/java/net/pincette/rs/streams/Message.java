package net.pincette.rs.streams;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a message with a key, a value and a timestamp, all of which are optional.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donné
 */
public class Message<K, V> {
  public final K key;
  public final Instant timestamp;
  public final V value;

  public Message() {
    this(null, null, null);
  }

  private Message(final K key, final V value, final Instant timestamp) {
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
  }

  public static <K, V> Message<K, V> message(final K key, final V value) {
    return new Message<>(key, value, null);
  }

  @Override
  public boolean equals(final Object other) {
    return this == other
        || Optional.of(other)
            .filter(Message.class::isInstance)
            .map(Message.class::cast)
            .filter(m -> Objects.equals(key, m.key) && Objects.equals(value, m.value))
            .isPresent();
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return "{\n  key: " + key + ",\n  value: " + value + "\n}";
  }

  public Message<K, V> withKey(final K key) {
    return new Message<>(key, value, timestamp);
  }

  public Message<K, V> withTimestamp(final Instant timestamp) {
    return new Message<>(key, value, timestamp);
  }

  public Message<K, V> withValue(final V value) {
    return new Message<>(key, value, timestamp);
  }
}
