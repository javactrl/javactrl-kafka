package io.github.javactrl.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.javactrl.ext.CRunnable;
import io.github.javactrl.rt.CThrowable;
import io.github.javactrl.rt.CallFrame;
import io.github.javactrl.rt.Ctrl;
import io.github.javactrl.rt.Unwind;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * State of the currently running workflow
 */
@Ctrl
class WorkflowContext implements Serializable {
  /** Serialization id */
  static final long serialVersionUID = CallFrame.serialVersionUID;

  /** Helper to retrieve current state */
  static final ThreadLocal<WorkflowContext> current = new ThreadLocal<>();

  /** Current threadId */
  final String threadId;

  /** Mapping of variable's local ids to their objects */
  final Map<String, Var> suspended = new HashMap<>();

  /** Variables counter for unique names generation */
  int varCount = 0;

  /** Is workflow function already exited */
  boolean done = false;

  /** Apache Kafka Streams API context */
  ProcessorContext<String, String> processorContext;

  /** The current workflow execution is a reaction on this recor */
  Record<String, String> origRecord;

  /** Workflow's function */
  final Workflow workflow;

  /** Logger */
  private static final Logger log = LoggerFactory.getLogger(Workflow.class);

  /**
   * Constructor 
   * 
   * @param threadId Current thread id
   * @param workflow Workflow's function
   */
  WorkflowContext(final String threadId, final Workflow workflow) {
    this.threadId = threadId;
    this.workflow = workflow;
  }

  /**
   * This stops workflow execution until it receives a record in a 
   * dedicated resumption topic (say, "workflow-resume")
   * 
   * The resumption topic record should have a key equal to the current thread 
   * id, and its value should be specially formatted. 
   * 
   * If the value is a string <code>v.getLocalId() + ":=" + resultValue</code> this 
   * <code>await</code> call returns this <code>resultValue</code>.
   * 
   * If the value <code>v.getLocalId() + ":!" + resultException</code> this
   * <code>await</code> call will throw an exception {@link WorkflowException} with
   * the </code>resultException<code> as its message.
   * 
   * @param v delayed result representation
   * @return normal result value (message with <code>":="</code> separator)
   * @throws CThrowable if suspended (always when this is called) or failed after resumption
   */
  String await(final Var v) throws CThrowable {
    try {
      suspended.put(v.getLocalId(), v);
      return Unwind.brk(v);
    } finally {
      suspended.remove(v.getLocalId());
    }
  }

  /**
   * Resume all known {@link Var} objects with an exception <code>value</code>
   * 
   * @param value an exception to throw
   */
  void resumeThrow(final Throwable value) {
    for (final var i : suspended.values()) {
      try {
        i.head.resumeThrow(value);
      } catch (final Throwable e) {
      }
    }
  }

  /**
   * Parsing the received record's value to either resume variable with an exception 
   * or some returned value
   * 
   * @param value the record's value
   */
  void resume(final String value) {
    final var sep = value.indexOf(':');
    final var varId = sep == -1 ? value : value.substring(0, sep);
    final var token = suspended.getOrDefault(varId, null);
    if (token == null) {
      log.debug("variable {} isn't suspended", varId);
      return;
    }
    final var arg = sep == -1 ? null : value.substring(sep + 2, value.length());
    final var kind = sep == -1 ? '=' : value.charAt(sep + 1);
    try {
      if (kind == '=') {
        token.head.resume(arg);
      } else if (kind == '!') {
        token.head.resumeThrow(new WorkflowException(arg));
      }
    } catch (final CThrowable t) {
    }
  }

  /** 
   * Entry point for workflow. 
   * 
   * A new workflow is started when resumption topic receives a message with a unique
   * thread id as its key and value as a string with <code>"new:"+argument</code> format.
   * 
   * @param argument The argument received in a message
   */
  void start(final String argument) {
    CRunnable.brackets(() -> {
      try {
        workflow.run(argument);
      } finally {
        done = true;
      }
    });
  }
}
