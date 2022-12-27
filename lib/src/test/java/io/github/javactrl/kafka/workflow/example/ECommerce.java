package io.github.javactrl.kafka.workflow.example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.javactrl.kafka.Scheduler;
import io.github.javactrl.kafka.WorkflowProcessorSupplier;
import io.github.javactrl.rt.CThrowable;
import io.github.javactrl.rt.Ctrl;

import static io.github.javactrl.ext.Concurrency.*;
import static io.github.javactrl.kafka.Workflow.*;

@Ctrl
public class ECommerce {
  public static void workflow(final String parameter) throws CThrowable {
    final int timeout = parameter == null ? 1000 : Integer.parseInt(parameter);
    final var items = new HashMap<String, Integer>();
    String email = null;
    var abandoned = false;
    for (;;) {
      final Message msg = anyOf(
        () -> next(), 
        () -> {
          Scheduler.sleep(timeout, TimeUnit.MILLISECONDS);
          return new Message(MessageType.timeout, null, null);
        });
      if (abandoned && msg.type != MessageType.timeout)
        abandoned = false;
      switch (msg.type) {
        case addToCart:
          items.compute(msg.item.productId, (k, v) -> (v == null ? 0 : v) + msg.item.quantity);
          break;
        case removeFromCart:
          final var q = msg.item.quantity;
          items.computeIfPresent(msg.item.productId, (k, v) -> q >= v ? null : v - q);
          break;
        case timeout:
          if (email == null || abandoned)
            return;
          abandoned = true;
          forward("reminder", email);
          break;
        case updateEmail:
          email = msg.email;
          break;
        case checkout:
          if (email == null) {
            forward("ecommerceError", "Must have email to check out!");
            break;
          }
          if (items.isEmpty()) {
            forward("Must have items to check out!", "checkoutError");
            break;
          }
          forwardJSON("checkout", new CartState(items, email));
          return;
        case getCart:
          forwardJSON("getCart", new CartState(items, email));
          break;
      }
    }
  }

  public static void main(final String[] args) throws Exception {
    final var bootstrapServers = args.length > 0 ? args[args.length-1] : "localhost:9092";
    final var config = new Properties();
    config.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "ecommerce-demo-workflow");
    config.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    config.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    final var topology = new Topology();
    topology.addSource("Loop", "workflow-resume")
      .addProcessor("Process", new WorkflowProcessorSupplier(ECommerce::workflow), "Loop")
      .addSink("scheduler", "workflow-scheduler", "Process")
      .addSink("getCart", "getCart", "Process")
      .addSink("checkout", "checkout", "Process")
      .addSink("ecommerceError", "ecommerceError", "Process")
      .addSink("reminder", "reminder", "Process");
    final var app = new KafkaStreams(topology, config);
    /* always reseting since it is a demo example, don't reset in production... */
    app.cleanUp();
    app.start();
    Runtime.getRuntime().addShutdownHook(new Thread(app::close));
  }

  static record CartItem(String productId, int quantity) implements Serializable {
  };

  enum MessageType {
    timeout, addToCart, removeFromCart, updateEmail, checkout, getCart
  }

  static record CartState(Map<String, Integer> items, String email) implements Serializable {
  };

  static record Message(MessageType type, String email, CartItem item) implements Serializable {
  };

  static Message next() throws CThrowable {
    try {
      final var v = newVar("main");
      final var msgStr = await(v);
      final var mapper = new ObjectMapper();
      // final var tree = mapper.readTree(msg);
      final Message msg = mapper.readValue(msgStr, Message.class);
      return msg;
    } catch (Exception e) {
      throw new RuntimeException("couldn't parse JSON message", e);
    }
  }

  static <T> void forwardJSON(String child, T obj) {
    final var mapper = new ObjectMapper();
    try {
      forward(child, mapper.writeValueAsString(obj));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("couldn't write a message", e);
    }
  }
}
