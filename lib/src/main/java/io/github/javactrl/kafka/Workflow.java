package io.github.javactrl.kafka;

import java.io.Serializable;

import org.apache.kafka.streams.processor.api.ProcessorContext;

import io.github.javactrl.rt.CThrowable;

/**
 * Functional interface for Kafka workflows definitions.
 */
@FunctionalInterface
public interface Workflow extends Serializable {

  /**
   * The method to implement (or a lambda) for the workflow definition. 
   * 
   * Workflow functions are just simple Java functions, but they can 
   * be suspended for a long time (days, months etc), and resumed 
   * after this. The function is converted into a plain Apache Kafka 
   * Streams Processor. 
   * 
   * @param parameter parameter received by the workflow's starting message
   * @throws CThrowable if suspends or there is an error
   */
  void run(String parameter) throws CThrowable;

  /** 
   * Getting current <code>org.apache.kafka.streams.processor.api.ProcessorContext</code> 
   * from Kafka Streams API 
   * 
   * @return the current processor context
   */
  public static ProcessorContext<String, String> getProcessorContext() {
    return WorkflowContext.current.get().processorContext;
  }

  /** 
   * Gets current thread  
   * 
   * @return thread id string
   */
  public static String getThreadId() {
    return WorkflowContext.current.get().threadId;
  }

  /**  
   * Generates a new variable on which workflows can suspend with an unique identifier
   * 
   * @return frash Var object
   */
  public static Var newVar() {
    final var context = WorkflowContext.current.get();
    return new Var(String.format("v%d", ++context.varCount));
  }

  /** 
   * Generates a new variable on which workflows can suspend with a specific local identifier
   * 
   * @param id the name of the variable
   * @return fresh Var object
   */
  public static Var newVar(final String id) {
    return new Var(id);
  }
  
  /**
   * A short-cut to <code>WorkflowContext.current.get().await(v)</code>
   * 
   * @see WorkflowContext#await(Var)
   * @param v Var object to suspend on
   * @return result value on resumption
   * @throws CThrowable if suspended (always when this is called) or failed after resumption
   */
  public static String await(final Var v) throws CThrowable {
    return WorkflowContext.current.get().await(v);
  }

  /** 
   * A short-cut to <code>WorkflowContext.current.get().await(newVar(name))</code>
   * 
   * @see WorkflowContext#await(Var)
   * @param id local identifier of a new Var object
   * @return result value on resumption
   * @throws CThrowable if suspended (always when this is called) or failed after resumption
   */
  public static String await(final String id) throws CThrowable {
    return await(newVar(id));
  }

  /** 
   * Forwards a record into registered Processor sink
   * 
   * @param child name of the sink
   * @param key record's name
   * @param value record's value
   */
  public static void forward(final String child, final String key, final String value) {
    final var context = WorkflowContext.current.get();
    context.processorContext.forward(context.origRecord.withKey(key).withValue(value), child);
  }

  /** 
   * Forwards a record into registered Processor sink, using the current thread id as 
   * the record's key
   * 
   * @param child name of the sink
   * @param value record's value
   */
  public static void forward(final String child, final String value) {
    forward(child, getThreadId(), value);
  }

}
