package org.rm3l.beam.firestore.impl;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractImplementation implements Serializable {

  protected final Logger logger = LoggerFactory.getLogger(this.getClass());

  public final void run(final String[] args) {
    doCreatePipeline(args).run().waitUntilFinish();
  }

  protected abstract Pipeline doCreatePipeline(final String[] args);
}
