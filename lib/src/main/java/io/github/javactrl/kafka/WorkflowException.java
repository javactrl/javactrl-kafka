package io.github.javactrl.kafka;

/** Just an exception to signal various workflow's problems */
public class WorkflowException extends RuntimeException {
  /**
   * Constructor 
   * 
   * @param message message
   */
  public WorkflowException(String message) {
    super(message);
  }
}
