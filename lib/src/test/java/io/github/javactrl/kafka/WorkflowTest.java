package io.github.javactrl.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.github.javactrl.kafka.workflow.example.ECommerce;
import io.github.javactrl.kafka.workflow.example.TripBookingSaga;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowTest {
  TopologyTestDriver testDriver;
  TestInputTopic<String, String> resumeTopic;
  TestOutputTopic<String, String> schedulerTopic;

  TestOutputTopic<String, String> ecommerceCheckoutTopic;
  TestOutputTopic<String, String> ecommerceErrorTopic;
  TestOutputTopic<String, String> ecommerceGetGartTopic;
  TestOutputTopic<String, String> ecommerceReminderTopic;

  TestOutputTopic<String, String> sagaResultTopic;
  TestOutputTopic<String, String> sagaReserveCarTopic;
  TestOutputTopic<String, String> sagaReserveHotelTopic;
  TestOutputTopic<String, String> sagaReserveFlightTopic;
  TestOutputTopic<String, String> sagaCancelCarTopic;
  TestOutputTopic<String, String> sagaCancelHotelTopic;
  TestOutputTopic<String, String> sagaCancelFlightTopic;
  TestOutputTopic<String, String> sagaErrorTopic;

  void setupECommerce() throws IOException {
    final var config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ecommerce-demo-workflow");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    final var topology = new Topology();
    topology.addSource("Loop", "workflow-resume")
        .addProcessor("Process", new WorkflowProcessorSupplier(ECommerce::workflow) {
          @Override
          public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("store"), Serdes.String(),
                Serdes.ByteArray()));
          }
        }, "Loop")
        .addSink("scheduler", "workflow-scheduler", "Process")
        .addSink("getCart", "getCart", "Process")
        .addSink("checkout", "checkout", "Process")
        .addSink("ecommerceError", "ecommerceError", "Process")
        .addSink("reminder", "reminder", "Process");
    testDriver = new TopologyTestDriver(topology, config);

    resumeTopic = testDriver.createInputTopic("workflow-resume", new StringSerializer(), new StringSerializer());
    schedulerTopic = testDriver.createOutputTopic("workflow-scheduler", new StringDeserializer(),
        new StringDeserializer());
    ecommerceCheckoutTopic = testDriver.createOutputTopic("checkout", new StringDeserializer(),
        new StringDeserializer());
    ecommerceErrorTopic = testDriver.createOutputTopic("checkoutError", new StringDeserializer(),
        new StringDeserializer());
    ecommerceGetGartTopic = testDriver.createOutputTopic("getCart", new StringDeserializer(),
        new StringDeserializer());
    ecommerceReminderTopic = testDriver.createOutputTopic("reminder", new StringDeserializer(),
        new StringDeserializer());
  }

  void setupTripBookingSaga() throws IOException {
    final var config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ecommerce-demo-workflow");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    final var topology = new Topology();
    topology.addSource("Loop", "saga-workflow-resume")
        .addProcessor("Process", new WorkflowProcessorSupplier(TripBookingSaga::workflow) {
          @Override
          public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("store"), Serdes.String(),
                Serdes.ByteArray()));
          }
        }, "Loop")
        .addSink("scheduler", "workflow-scheduler", "Process")
        .addSink("reserve-car", "saga-reserve-car", "Process")
        .addSink("cancel-car", "saga-cancel-car", "Process")
        .addSink("reserve-hotel", "saga-reserve-hotel", "Process")
        .addSink("cancel-hotel", "saga-cancel-hotel", "Process")
        .addSink("reserve-flight", "saga-reserve-flight", "Process")
        .addSink("cancel-flight", "saga-cancel-flight", "Process")
        .addSink("error", "saga-error", "Process")
        .addSink("result", "saga-result", "Process");
    testDriver = new TopologyTestDriver(topology, config);

    resumeTopic = testDriver.createInputTopic("saga-workflow-resume", new StringSerializer(), new StringSerializer());
    schedulerTopic = testDriver.createOutputTopic("workflow-scheduler", new StringDeserializer(),
        new StringDeserializer());
    sagaResultTopic = testDriver.createOutputTopic("saga-result", new StringDeserializer(),
        new StringDeserializer());
    sagaErrorTopic = testDriver.createOutputTopic("saga-error", new StringDeserializer(),
        new StringDeserializer());
    sagaReserveCarTopic = testDriver.createOutputTopic("saga-reserve-car", new StringDeserializer(),
        new StringDeserializer());
    sagaReserveHotelTopic = testDriver.createOutputTopic("saga-reserve-hotel", new StringDeserializer(),
        new StringDeserializer());
    sagaReserveFlightTopic = testDriver.createOutputTopic("saga-reserve-flight", new StringDeserializer(),
        new StringDeserializer());
    sagaCancelCarTopic = testDriver.createOutputTopic("saga-cancel-car", new StringDeserializer(),
        new StringDeserializer());
    sagaCancelHotelTopic = testDriver.createOutputTopic("saga-cancel-hotel", new StringDeserializer(),
        new StringDeserializer());
    sagaCancelFlightTopic = testDriver.createOutputTopic("saga-cancel-flight", new StringDeserializer(),
        new StringDeserializer());
  }

  @AfterEach
  public void tearDown() {
    try {
      testDriver.close();
    } catch (final RuntimeException e) {
      // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when
      // executed in Windows, ignoring it
      // Logged stacktrace cannot be avoided
      System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
    }
  }

  /**
  * Running a simple ecommerce workflow
  */
  @Test
  void testECommerce() throws IOException {
    setupECommerce();
    final var store = testDriver.getKeyValueStore("store");
    resumeTopic.pipeInput("thread1", "new:2000");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"updateEmail\",\"email\":\"someone@example.com\"}");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"addToCart\",\"item\":{\"quantity\":10,\"productId\":\"teapot\"}}");
    resumeTopic.pipeInput("thread2", "new:1000");
    resumeTopic.pipeInput("thread2",
        "main:={\"type\":\"updateEmail\",\"email\":\"vitaliy.akimov@gmail.com\"}");
    resumeTopic.pipeInput("thread3", "new:3000");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"addToCart\",\"item\":{\"quantity\":11,\"productId\":\"sigar\"}}");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"addToCart\",\"item\":{\"quantity\":20,\"productId\":\"teapot\"}}");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"updateEmail\",\"email\":\"vitaliy.akimov@gmail.com\"}");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"removeFromCart\", \"item\":{\"quantity\":11,\"productId\":\"sigar\"}}");
    resumeTopic.pipeInput("thread1",
        "main:={\"type\":\"addToCart\",\"item\":{\"quantity\":2,\"productId\":\"sugar\"}}");
    resumeTopic.pipeInput("thread1", "main:={\"type\":\"getCart\"}");
    assertEquals(
        new KeyValue<>("thread1", "{\"items\":{\"teapot\":30,\"sugar\":2},\"email\":\"vitaliy.akimov@gmail.com\"}"),
        ecommerceGetGartTopic.readKeyValue());
    resumeTopic.pipeInput("thread1", "main:={\"type\":\"checkout\"}");
    assertEquals(
        new KeyValue<>("thread1", "{\"items\":{\"teapot\":30,\"sugar\":2},\"email\":\"vitaliy.akimov@gmail.com\"}"),
        ecommerceCheckoutTopic.readKeyValue());
    final var scheduled = schedulerTopic.readKeyValuesToMap();
    assertEquals("1000", scheduled.get("thread2|v2"));
    assertEquals("3000", scheduled.get("thread3|v1"));
    assertNull(store.get("thread1"));
    assertNotNull(store.get("thread2"));
    assertNotNull(store.get("thread3"));
    resumeTopic.pipeInput("thread2", "v2:=");
    resumeTopic.pipeInput("thread3", "v1:=");
    assertNotNull(store.get("thread2"));
    assertNull(store.get("thread3"));
    assertTrue(ecommerceCheckoutTopic.isEmpty());
    final var reminder = ecommerceReminderTopic.readKeyValuesToList();
    assertIterableEquals(List.of(new KeyValue<>("thread2", "vitaliy.akimov@gmail.com")), reminder);
    final var scheduled2 = schedulerTopic.readKeyValuesToMap();
    assertEquals("1000", scheduled2.get("thread2|v3"));
    assertEquals(1, scheduled2.size());
    resumeTopic.pipeInput("thread2", "v3:=");
    assertTrue(ecommerceErrorTopic.isEmpty());
    try (final var storeIter = store.all()) {
      assertFalse(storeIter.hasNext(), "everything has exited");
    }
  }

  @Test
  public void testTripBookingSagaAllResolved() throws IOException {
    setupTripBookingSaga();
    resumeTopic.pipeInput("thread1", "new:{}");
    resumeTopic.pipeInput("thread1", String.format("%s:=PS124", sagaReserveFlightTopic.readValue()));
    resumeTopic.pipeInput("thread1", String.format("%s:=CityInn", sagaReserveHotelTopic.readValue()));
    resumeTopic.pipeInput("thread1", String.format("%s:=EconomySedane", sagaReserveCarTopic.readValue()));
    assertEquals(
        new KeyValue<>("thread1", "EconomySedane,CityInn,PS124"),
        sagaResultTopic.readKeyValue());
    assertTrue(sagaResultTopic.isEmpty());
    assertTrue(sagaCancelCarTopic.isEmpty());
    assertTrue(sagaCancelHotelTopic.isEmpty());
    assertTrue(sagaCancelFlightTopic.isEmpty());
    try (final var storeIter = testDriver.getKeyValueStore("store").all()) {
      assertFalse(storeIter.hasNext(), "everything has exited");
    }
  }

  @Test
  public void testTripBookingSagaError() throws IOException {
    setupTripBookingSaga();
    resumeTopic.pipeInput("thread1", "new:");
    resumeTopic.pipeInput("thread1", String.format("%s:=PS124", sagaReserveFlightTopic.readValue()));
    resumeTopic.pipeInput("thread1", String.format("%s:!no hotels available", sagaReserveHotelTopic.readValue()));
    assertEquals(new KeyValue<>("thread1", sagaReserveCarTopic.readValue()), sagaCancelCarTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", "PS124"), sagaCancelFlightTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", "no hotels available"), sagaErrorTopic.readKeyValue());
    assertTrue(sagaErrorTopic.isEmpty());
    assertTrue(sagaResultTopic.isEmpty());
    assertTrue(sagaCancelCarTopic.isEmpty());
    assertTrue(sagaCancelHotelTopic.isEmpty());
    assertTrue(sagaCancelFlightTopic.isEmpty());
    try (final var storeIter = testDriver.getKeyValueStore("store").all()) {
      assertFalse(storeIter.hasNext(), "everything has exited");
    }
  }

  @Test
  public void testTripBookingSagaErrorInCode() throws IOException {
    setupTripBookingSaga();
    resumeTopic.pipeInput("thread1", "new:throwInFlight");
    resumeTopic.pipeInput("thread1", String.format("%s:=CityInn", sagaReserveHotelTopic.readValue()));
    resumeTopic.pipeInput("thread1", String.format("%s:=PS124", sagaReserveFlightTopic.readValue()));
    final var carTaskId = sagaReserveCarTopic.readValue();
    resumeTopic.pipeInput("thread1", String.format("%s:=EconomySedane", carTaskId));
    assertEquals(new KeyValue<>("thread1", "something is wrong"), sagaErrorTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", carTaskId), sagaCancelCarTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", "CityInn"), sagaCancelHotelTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", "PS124"), sagaCancelFlightTopic.readKeyValue());
    assertTrue(sagaErrorTopic.isEmpty());
    assertTrue(sagaResultTopic.isEmpty());
    assertTrue(sagaCancelCarTopic.isEmpty());
    assertTrue(sagaCancelHotelTopic.isEmpty());
    assertTrue(sagaCancelFlightTopic.isEmpty());
    try (final var storeIter = testDriver.getKeyValueStore("store").all()) {
      assertFalse(storeIter.hasNext(), "everything has exited");
    }
  }

  @Test
  public void testTripBookingSagaTimeout() throws IOException {
    setupTripBookingSaga();
    resumeTopic.pipeInput("thread1", "new:");
    resumeTopic.pipeInput("thread1", String.format("%s:=PS124", sagaReserveFlightTopic.readValue()));
    resumeTopic.pipeInput("thread1", String.format("%s:=EconomySedane", sagaReserveCarTopic.readValue()));
    resumeTopic.pipeInput("thread1", String.format("%s:=",schedulerTopic.readKeyValue().key.split("\\|")[1]));
    assertEquals(new KeyValue<>("thread1", "timeout"), sagaErrorTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", "EconomySedane"), sagaCancelCarTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", sagaReserveHotelTopic.readValue()), sagaCancelHotelTopic.readKeyValue());
    assertEquals(new KeyValue<>("thread1", "PS124"), sagaCancelFlightTopic.readKeyValue());
    assertTrue(sagaErrorTopic.isEmpty());
    assertTrue(sagaResultTopic.isEmpty());
    assertTrue(sagaCancelCarTopic.isEmpty());
    assertTrue(sagaCancelHotelTopic.isEmpty());
    assertTrue(sagaCancelFlightTopic.isEmpty());
    try (final var storeIter = testDriver.getKeyValueStore("store").all()) {
      assertFalse(storeIter.hasNext(), "everything has exited");
    }
  }

}
