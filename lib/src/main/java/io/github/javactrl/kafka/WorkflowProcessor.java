package io.github.javactrl.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * This is the implementation of Apache Kafka Streams 
 * <code>org.apache.kafka.streams.processor.api.Processor</code>
 * 
 * This Processor expects a single source with its key and value strings. 
 * And key-value store (which is called "store") for storing the workflow's 
 * state. It's recommended to use {@link WorkflowProcessorSupplier}, 
 * which already adds a store. The store must have string keys and <code>byte[]</code>
 * values.
 * 
 * Each sink to which the workflow writes must be added as a sink to this Processor.
 */
public class WorkflowProcessor implements Processor<String, String, String, String> {
  /** A prefix for messages on resumption thread which starts a new worklow */
  public static final String NEW_THREAD_PREFIX = "new:";

  /** Logger */
  private static final Logger log = LoggerFactory.getLogger(Workflow.class);

  /** The key value store (called "store") */
  private KeyValueStore<String, byte[]> store;

  /** ProcessorContext from Kafka Streams API */
  private ProcessorContext<String, String> processorContext;

  /** The currently running workflow function */
  private Workflow workflow;

  /**
   * Constructor
   * 
   * @param workflow the workflow function to run on this Processor
   */
  public WorkflowProcessor(Workflow workflow) {
    this.workflow = workflow;
  }

  @Override
  public void init(final ProcessorContext<String, String> context) {
    store = context.getStateStore("store");
    processorContext = context;
  }

  @Override
  public void process(Record<String, String> record) {
    log.debug("process: key:{}, task:{}", record.key(), processorContext.taskId());
    final var threadId = record.key();
    var argument = record.value();
    final var newThread = argument.startsWith(NEW_THREAD_PREFIX);
    WorkflowContext context = null;
    try {
      if (newThread) {
        WorkflowContext newContext = new WorkflowContext(threadId, workflow);
        context = newContext;
        newContext.processorContext = processorContext;
        newContext.origRecord = record;
        WorkflowContext.current.set(newContext);
        context.start(argument.substring(NEW_THREAD_PREFIX.length()));
      } else {
        final var state = store.get(threadId);
        if (state == null) {
          log.warn("not available thread {}", threadId);
          return;
        }
        try (final var bytesStream = new ByteArrayInputStream(state);
            final var objectsStream = new ObjectInputStream(bytesStream)) {
          context = (WorkflowContext) objectsStream.readObject();
        }
        context.processorContext = processorContext;
        context.origRecord = record;
        WorkflowContext.current.set(context);
        context.resume(argument);
      }
      if (context.done) {
        store.delete(threadId);
      } else {
        context.processorContext = null;
        context.origRecord = null;
        try (final var bytesStream = new ByteArrayOutputStream();
            final var objectStream = new ObjectOutputStream(bytesStream)) {
          objectStream.writeObject(context);
          store.put(threadId, bytesStream.toByteArray());  
        }
      }
    } catch (Throwable e) {
      log.error("workflow step error", e);
      if (context != null) {
        context.resumeThrow(e);
      }
    } finally {
      WorkflowContext.current.set(null);
    }
  }
}
