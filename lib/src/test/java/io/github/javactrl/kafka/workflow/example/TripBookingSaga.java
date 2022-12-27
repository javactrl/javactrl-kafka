package io.github.javactrl.kafka.workflow.example;

import io.github.javactrl.ext.CSupplier;
import io.github.javactrl.kafka.Scheduler;
import io.github.javactrl.kafka.WorkflowException;
import io.github.javactrl.kafka.WorkflowProcessorSupplier;
import io.github.javactrl.rt.CThrowable;
import io.github.javactrl.rt.Ctrl;

import static io.github.javactrl.ext.Concurrency.*;
import static io.github.javactrl.kafka.Workflow.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

@Ctrl
public class TripBookingSaga {
  static final int TIMEOUT_MS = 1000;

  public static void workflow(final String parameter) throws CThrowable {
    final var compensations = new ArrayList<CSupplier<Void>>();
    try {
      final List<String> ret = anyOf(
          () -> {
            Scheduler.sleep(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            throw new RuntimeException("timeout");
          },
          () -> allOf(
              () -> {
                final var car = reserveCar();
                compensations.add(() -> {
                  cancelCar(car);
                  return null;
                });
                return car;
              },
              () -> {
                final var hotel = reserveHotel();
                compensations.add(() -> {
                  cancelHotel(hotel);
                  return null;
                });
                return hotel;
              },
              () -> {
                final var flight = reserveFlight();
                compensations.add(() -> {
                  cancelFlight(flight);
                  return null;
                });
                if (parameter.equals("throwInFlight"))
                  throw new RuntimeException("something is wrong");
                return flight;
              }));
      /* .... do anything with the ids */
      forward("result", String.join(",", ret));
    } catch (final Throwable t) {
      forward("error", t.getMessage());
      allOf(compensations);
    }
  }

  public static void main(final String[] args) throws Exception {
    final var bootstrapServers = args.length > 0 ? args[args.length - 1] : "localhost:9092";
    final var config = new Properties();
    config.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "trip-booking-saga-demo-workflow");
    config.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    final var topology = new Topology();
    topology.addSource("Loop", "workflow-resume")
        .addProcessor("Process", new WorkflowProcessorSupplier(TripBookingSaga::workflow), "Loop")
        .addSink("scheduler", "workflow-scheduler", "Process")
        .addSink("reserve-car", "saga-reserve-car", "Process")
        .addSink("cancel-car", "saga-cancel-car", "Process")
        .addSink("reserve-hotel", "saga-reserve-hotel", "Process")
        .addSink("cancel-hotel", "saga-cancel-hotel", "Process")
        .addSink("reserve-flight", "saga-reserve-flight", "Process")
        .addSink("cancel-flight", "saga-cancel-flight", "Process")
        .addSink("error", "saga-error", "Process")
        .addSink("result", "saga-result", "Process");
    final var app = new KafkaStreams(topology, config);
    /* always reseting since it is a demo example, don't reset in production... */
    app.cleanUp();
    app.start();
    Runtime.getRuntime().addShutdownHook(new Thread(app::close));

  }

  static String reserveCar() throws CThrowable {
    return reserve("car");
  }

  static void cancelCar(String id) throws CThrowable {
    cancel("car", id);
  }

  static String reserveHotel() throws CThrowable {
    return reserve("hotel");
  }

  static void cancelHotel(String id) throws CThrowable {
    cancel("hotel", id);
  }

  static String reserveFlight() throws CThrowable {
    return reserve("flight");
  }

  static void cancelFlight(String id) throws CThrowable {
    cancel("flight", id);
  }

  static void cancel(String what, String id) throws CThrowable {
    forward("cancel-" + what, id);
  }

  static String reserve(String what) throws CThrowable {
    final var v = newVar();
    forward("reserve-" + what, v.getLocalId());
    try {
      return await(v);
    } catch (CancellationException e) {
      cancel(what, v.getLocalId());
      throw e;
    }
  }

  static void timeout() throws CThrowable {
    final var v = newVar();
    final var key = String.format("%s|%s", getThreadId(), v.getLocalId());
    forward("scheduler", key, Integer.toString(TIMEOUT_MS));
    try {
      await(v);
    } catch (Throwable t) {
      forward("scheduler", key, "0");
      return;
    }
    throw new WorkflowException("timeout");
  }

}
