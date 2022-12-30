# Kafka Workflow on JVM

![CI](https://github.com/javactrl/javactrl-kafka/actions/workflows/main.yml/badge.svg)
[![javadoc](https://javadoc.io/badge2/io.github.javactrl/javactrl-kafka/javadoc.svg)](https://javadoc.io/doc/io.github.javactrl/javactrl-kafka)

The project is a new way to define workflows as code. It's minimalistic but feature-complete. It works on JVM with access to anything third-party available for JVM and  Kafka Streams.

Workflows here are just small and straightforward direct-style Java functions and they are converted into Apache Kafka Streams nodes. Kafka takes all the burden of making such workflows highly available, scalable, durable, fault-tolerant, distributed and so on. In addition, workflows are simple, easy to read, easy to write, easy to debug, easy to maintain, and easy to integrate with other components of Kafka-based infrastructure.

Typical use cases include:

* Business Process Automation
* Microservices Orchestration
* Distributed Transactions
* Infrastructure Provisioning
* Monitoring and Polling
* Data Pipelines

Apache Kafka is famous for solving these tasks already. However, it requires small event handlers, which are rather difficult to develop, test, maintain and change. For example, it's uncomplicated to introduce small unintended dependencies between the handlers. Tools such as ksqldb help a lot, but they still aren't intended to specify the sequential essence of workflow dependencies. This doesn't mean this library is a kind of replacement for ksqldb. Instead, users can benefit from operating them together. Say, ksqldb as a rules engine, and javactrl as a workflow engine.

The subsequent typical resort is to define workflows as state machines, but such definitions are too low-level, and the state's complexity quickly explodes to unmanageable values. We don't program everything in state machines, after all. Still, the programs we develop can be converted to state machines for analysis or optimization purposes.

To make such state machines more human-usable, some so-called No-Code approaches are often applied. They use visual language with a graphical diagram editor, a spreadsheet, or some declarative DSLs in .yaml/.xml/.json/plain text, etc. No-Code is not the proper term here. Diagrams and spreadsheets are also code but in a bit different form. They may indeed improve readability somehow, but they don't reduce complexity. They are readable only until some complexity threshold, after which they are a mess requiring applying typical programming techniques, which developers use to improve their programs.

On the other hand, the dependencies between the steps are evident if the whole workflow is defined as a Java function. I mean a single Java function for all workflow steps, not many small Java event handlers. Such Java code maps one-to-one domain logic to implementation. No need for non-executable complex diagrams or documentation before development. Develop, model, and document everything in one step.

In async programming, this corresponds to the async functions, which are proven to make programs much simpler compared to callbacks passing. 

There are similar code-based orchestration tools, for example (Uber Cadence or Temporal.io). However, JavaCtrl usage is much simpler - no learning of new concepts (such as Singal, Action, Workflow etc.), and no dedicated IT infrastructure components are required. It's just plain Java plus Kafka Streams.

Here is a workflow example:

```java
  var compensations = new ArrayList<CRunnable>();
  try {
    var car = reserveCar();
    compensations.add(() -> {
      cancelCar(car);
    });
    var hotel = reserveHotel();
    compensations.add(() -> {
      cancelHotel(hotel);
    });    
    var flight = reserveFlight();
    compensations.add(() -> {
      cancelFlight(flight);
    });
  } catch (Throwable t) {
    for(var i : compensations) {
      i.run();
    }
  }
```

The execution is suspended in any of `reserveCar`/`reserveHotel`/`reserveFlight` until some service confirms or denies the corresponding action. After which it cancels everything which could be reserved before.

These three steps can run in parallel, and there are indeed combinators to run the part of code in parallel, like this:

```java
    var compensations = new ArrayList<CSupplier<Void>>();
    try {
      List<String> ret = anyOf(
          () -> {
            Scheduler.sleep(TIMEOUT_DAYS, TimeUnit.DAYS);
            throw new RuntimeException("timeout");
          },
          () -> allOf(
              () -> {
                var car = reserveCar();
                compensations.add(() -> {
                  cancelCar(car);
                  return null;
                });
                return car;
              },
              () -> {
                var hotel = reserveHotel();
                compensations.add(() -> {
                  cancelHotel(hotel);
                  return null;
                });
                return hotel;
              },
              () -> {
                var flight = reserveFlight();
                compensations.add(() -> {
                  cancelFlight(flight);
                  return null;
                });
                if (parameter.equals("throwInFlight"))
                  throw new RuntimeException("something is wrong");
                return flight;
              }));
      /* .... do anything with the ids */
      forward("result", "return:%s".formatted(String.join(",", ret)));
    } catch (Throwable t) {
      forward("error", "%s".formatted(t.getMessage()));
      allOf(compensations);
    }
```

Here `anyOf` awaits either some timeout or all three things are reserved. In case of timeout, it throws an exception, so even if something was booked, it will be canceled after.

Any design technique or pattern is available at your expense, along with any JVM library, framework, tool, or platform. No vendor lock-in. Integrate easily into your infrastructure. Be more productive and agile.

## Usage

The library is based on [javactrl](https://github.com/javactrl/javactrl) continuations library. Any other continuation library with serializable continuations can be used instead. But javactrl has an exception handler for simplifying managing resources. In some following versions, the multi-shot feature may be utilized to implement something like DB transactions.

This continuations library requires JVM byte code instrumentation. Either using Java Agent or ahead of time, like [javactrl](https://github.com/javactrl/javactrl).

Here's an example Gradle (Groovy) "build.gradle" with the workflow stream processor defined in `my.workflow.App`:

```gradle
plugins {
    id 'application'
}

repositories {
    mavenCentral()
}

configurations {
    javactrl
}

dependencies {
  javactrl 'io.github.javactrl:javactrl-core:1.0.2'
  implementation 'io.github.javactrl:javactrl-core:1.0.2'
  implementation 'io.github.javactrl:javactrl-kafka:1.0.1'
  implementation 'org.apache.kafka:kafka-streams:3.3.1'
  implementation 'org.slf4j:slf4j-simple:2.0.5' // or any other binding/provider for SLF4J
}

application {
    mainClass = 'my.workflow.App'
    applicationDefaultJvmArgs = ["-javaagent:${configurations.javactrl.iterator().next()}"]
}

test {
  useJUnitPlatform()
  jvmArgs "-javaagent:${configurations.javactrl.iterator().next()}"
}

```

With this "build.gradle" to start the stream processor just run `gradle run`, or generate a start script.

There is a [WorkflowProcessorSupplier](https://javadoc.io/doc/io.github.javactrl/javactrl-kafka/latest/io/github/javactrl/kafka/WorkflowProcessorSupplier.html) class to add Apache Kafka Streams Processor into your topology. This supplier expects as [Workflow](https://javadoc.io/doc/io.github.javactrl/javactrl-kafka/latest/io/github/javactrl/kafka/Workflow.html) functional interface as its argument. This is the actual workflow definition. It takes workflow start parameters and can be suspended anywhere (except constructors).

The Processor expects a single source topic and any number of sink topics. The source topic, in the current version, should have string keys and values. The string type and the formats may be abstracted away in the following versions.

A new workflow thread starts when the stream processor receives a record with a unique thread id (this must be generated outside) and a value with `"new:"` prefix. The value after this prefix is passed as an argument to the workflow function.

Workflows can suspend workflow variables ([Var](https://javadoc.io/doc/io.github.javactrl/javactrl-kafka/latest/io/github/javactrl/kafka/Var.html) class), and each variable has a local id that can be forwarded to any application-specific topic to trigger some application-specific operation.

These operations may be remote microservice, a job scheduler, or even a request for human interactions by e-mail or a web link.

The workflow calls [get](https://javadoc.io/doc/io.github.javactrl/javactrl-kafka/latest/io/github/javactrl/kafka/Var.html#get()) method to suspend and wait for the result, which can arrive after days, months, etc.

The external handler of the requested operation should store thread and variable identifiers somewhere. After this remote operation is completed, the application-specific handling service must put the result into the source topic of this workflow processor.

The key in the topic must be the thread identifier again, and the value must have the format `<Var id>:=<resultValue>`. The workflow processor will resume executing, passing `<resultValue>` as the returned value of the suspended `get` method call.

If the remote handler needs to signal some exceptional situation, it posts a record with values like  `<Var id>:!<resultException>`. And in this case, the workflow will be resumed by throwing a [WorkflowException](https://javadoc.io/doc/io.github.javactrl/javactrl-kafka/latest/io/github/javactrl/kafka/WorkflowException.html) with the `<resultException>` message.

Often, workflows aren't meant to run sequentially only, and something must be done in parallel. There are [allOf](https://javadoc.io/doc/io.github.javactrl/javactrl-core/latest/io/github/javactrl/ext/Concurrency.html#allOf(java.util.List)), [anyOf](https://javadoc.io/doc/io.github.javactrl/javactrl-core/latest/io/github/javactrl/ext/Concurrency.html#anyOf(java.util.List)) continuations combinators for this. They accept a set of components to run simultaneously, awaiting all or any of them to finish before resuming. Any workflow can also start and communicate another workflow by posting into the related source topic.

Serializability of the continuations, of course, implies serializability of all local variables and all values reachable from them. Some objects cannot be serialized. For example, DB connections, sockets, authentication tokens, and more. But we can write convert the object into some placeholder when writing and re-connect, re-create, and re-authenticate on resuming. These can be done with a special Serializable instance, for example. Or it may be some code inside `Wind`/`Unwind` `catch` handlers.

Here is an example `main` converting workflow function `App::workflow` into a streams processor:

```java
  public static void main(String[] args) throws Exception {
    var bootstrapServers = args.length > 0 ? args[args.length-1] : "localhost:9092";
    var config = new Properties();
    config.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "app-workflow");
    config.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    config.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    var topology = new Topology();
    topology.addSource("Loop", "workflow-resume")
      .addProcessor("Process", new WorkflowProcessorSupplier(App::workflow), "Loop")
      .addSink("scheduler", "workflow-scheduler", "Process")
      .addSink("result", "result", "Process"); // ...
    var app = new KafkaStreams(topology, config);
    /* always reseting since it is a demo example, don't reset in production... */
    app.cleanUp();
    app.start();
    Runtime.getRuntime().addShutdownHook(new Thread(app::close));
  }
```

So this is just a usual Kafka Streams boilerplate. Add `addSink` for each destination topic this workflow can write.

## Testing and debugging

Since the workflow is just a plain Kafka Streams Processor API node, it can be tested the same way like any processors, namely, using `org.apache.kafka.streams.processor.api.MockProcessorContext.MockProcessorContext` from `kafka-streams-test-utils`.

The usual Java debugger should still work after instrumentation. It may behave weirdly on steppings, but breakpoints and variable values views work well most of the time. 

There is another advantage, though. We can have time-traveling debugging since we can load past states from the storage topic changelog.

## Links

There is an alternative implementation with JavaScript as workflow scripts - [kafka-workflow](https://github.com/awto/kafka-workflow).
