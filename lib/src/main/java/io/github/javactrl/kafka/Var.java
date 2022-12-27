package io.github.javactrl.kafka;

import io.github.javactrl.rt.CThrowable;
import io.github.javactrl.rt.Unwind;

/** 
 * This represents a result of suspended continuation (similar to Java <code>Future</code>)
 * 
 * @see WorkflowContext#await(Var)
 */
public class Var extends Unwind {
  /** local identifier of the variable */
  private final String id;

  /**
   * Gets identifier of this variable. Local ids are unique within thread's scope.
   * 
   * @return local identifier
   */
  public String getLocalId() {
    return id;
  }

  /**
   * Gets unique identifier containing information about its thread and its local id
   * 
   * @return full identifier
   */
  public String getFullId() {
    return String.format("%s|%s", Workflow.getThreadId(), id);
  }

  /**
   * Constructor
   * 
   * @param id local identifier
   */
  Var(final String id) {
    super();
    this.id = id;
  }

  /**
   * Gets result of this variable
   * 
   * This is a short-cut to <code>WorkflowContext.current.get().await(this)</code>
   * 
   * @see WorkflowContext#await(Var)
   * @return result
   * @throws CThrowable always to suspend the execution
   */
  public Object get() throws CThrowable {
    return Workflow.await(this);
  }
}
