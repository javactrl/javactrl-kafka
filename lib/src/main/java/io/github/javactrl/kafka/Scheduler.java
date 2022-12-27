package io.github.javactrl.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import io.github.javactrl.rt.CThrowable;
import io.github.javactrl.rt.Ctrl;

/**
 * Demo-only, not for production time scheduler. Write a record with a 
 * string key "{first part}|{second part}" and a delay value number (as a string). 
 * It will post a record with key "{first part}" and value
 * "{second part}" after the original record timestamp plus the received 
 * delay.
 * 
 * If the delay value is "0" the corresponding job will be canceled.
 */
@Ctrl
public class Scheduler {

  /**
   * Suspends the current thread execution for the specified amount of time.
   * 
   * @param duration duration's value
   * @param durationUnits duration's unit
   * @throws CThrowable always throws, since this always suspends
   */
  public static void sleep(final long duration, final TimeUnit durationUnits) throws CThrowable {
    final var v = Workflow.newVar();
    Workflow.forward("scheduler", v.getFullId(), Long.toString(durationUnits.toMillis(duration)));
    try {
      Workflow.await(v);
    } catch (Throwable t) {
      Workflow.forward("scheduler", v.getFullId(), "0");
      throw t;
    }
  }


  /** Apach Kafka Streams Processor implementing the workflow run implementation  */
  public static class SchedulerProcessor implements Processor<String, String, String, String> {

    /** storing context from Apacke Kafka Streams API */
    private ProcessorContext<String, String> context;
    
    /** the scheduler cancelation handler */
    private Cancellable cancel;
    /** mapping from delays to variables */
    private KeyValueStore<Long, List<String>> fwd;
    /** mapping from variables to delays */
    private KeyValueStore<String, Long> back;

    @Override
    public void init(final ProcessorContext<String, String> context) {
      this.context = context;
      cancel = context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME,
          this::punctuate);
      fwd = context.getStateStore("fwd");
      back = context.getStateStore("back");
    }

    /** 
     * Punctuation handler
     * 
     * @param timestamp interval
     */
    private void punctuate(final long timestamp) {
      try (final var iter = fwd.range(0L, timestamp)) {
        while (iter.hasNext()) {
          final var susp = iter.next();
          fwd.delete(susp.key);
          for (final var dest : susp.value) {
            final var destAddr = dest.split("\\|");
            context.forward(new Record<>(destAddr[0], destAddr[1], timestamp));
          }
        }
      }
    }

    @Override
    public void process(final Record<String, String> record) {
      final var addr = record.key();
      final var valueText = record.value();
      final var param = valueText == null ? 0L : Long.parseLong(valueText);
      if (param == 0) {
        final var timestamp = back.get(addr);
        if (timestamp != null) {
          back.delete(addr);
          final var tup = fwd.get(timestamp);
          if (tup != null) {
            tup.remove(addr);
            if (tup.isEmpty()) {
              fwd.delete(timestamp);
            } else {
              fwd.put(timestamp, tup);
            }
          }
        }
        return;
      }
      final long timestamp = record.timestamp() + param;
      var scheduled = fwd.get(timestamp);
      if (scheduled == null)
        scheduled = new ArrayList<String>();
      scheduled.add(addr);
      fwd.put(timestamp, scheduled);
      back.put(addr, timestamp);
    }

    @Override
    public void close() {
      cancel.cancel();
    }
  }

  /** 
   * Command line entry point
   * 
   * Expects bootstrap servers address or it's "localhost:9092" if not specified.
   * 
   * @param args command line arguments
   */
  @SuppressWarnings("unchecked")
  public static void main(final String[] args) {
    final var bootstrapServers = args.length > 0 ? args[args.length-1] : "localhost:9092";
    final var config = new Properties();
    config.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "workflow-demo-scheduler");
    config.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    config.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    final var topology = new Topology();
    topology.addSource("SchedulerEvents", "workflow-scheduler")
        .addProcessor("SchedulerProcessor", () -> new SchedulerProcessor(), "SchedulerEvents")
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("back"),
            Serdes.String(),
            Serdes.Long()), "SchedulerProcessor")
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("fwd"),
            Serdes.Long(),
            Serdes.ListSerde((Class<List<String>>)((Class<?>)ArrayList.class), Serdes.String())), "SchedulerProcessor")
        .addSink("WorkflowResume", "workflow-resume", "SchedulerProcessor");
    final var app = new KafkaStreams(topology, config);
    /* this shouldn't be used in production anyway */
    app.cleanUp();
    Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    app.start();
  }

}
