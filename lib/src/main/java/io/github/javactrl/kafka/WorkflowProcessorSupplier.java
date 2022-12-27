package io.github.javactrl.kafka;

import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * A helper class which produces {@link WorkflowProcessor} and also attach a store to it
 */
public class WorkflowProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

  /** The workflow function to run on the produced Processor */
  private final Workflow workflow;

  /**
   * Constructor
   * 
   * @param workflow workflow function to run on the produced Processor
   */
  public WorkflowProcessorSupplier(Workflow workflow) {
    this.workflow = workflow;
  }

  @Override
  public Processor<String, String, String, String> get() {
    return new WorkflowProcessor(workflow);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return Set.of(Stores
        .keyValueStoreBuilder(
            Stores.persistentKeyValueStore("store"),
            Serdes.String(),
            Serdes.ByteArray()));
  }
}
