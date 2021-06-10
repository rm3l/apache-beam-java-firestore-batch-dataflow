package org.rm3l.beam.firestore;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.rm3l.beam.firestore.impl.AbstractImplementation;
import org.rm3l.beam.firestore.impl.BatchWriteImplementation;
import org.rm3l.beam.firestore.impl.NaiveImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountToFirestorePipeline {

  private static final Logger logger = LoggerFactory
      .getLogger(WordCountToFirestorePipeline.class);

  public static void main(final String[] args) throws Exception {

    final long start = System.nanoTime();

    final Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    try {
      Optional.ofNullable(options.getImplementation())
          .map(Implementation::valueOf)
          .map(implementation -> implementation.implementationType)
          .orElseThrow(() -> new UnsupportedOperationException(
              "Not implemented yet: '" + options.getImplementation() + "'"))
          .getDeclaredConstructor()
          .newInstance()
          .run(args);
    } finally {
      final long end = System.nanoTime();
      logger.info("Done running '{}' implemention in {} nanos ({} ms)",
          options.getImplementation(),
          end - start,
          TimeUnit.NANOSECONDS.toMillis(end - start));
    }
  }

  public enum Implementation {
    naive(NaiveImplementation.class),

    batch(BatchWriteImplementation.class);

    public final Class<? extends AbstractImplementation> implementationType;

    Implementation(final Class<? extends AbstractImplementation> implementationType) {
      this.implementationType = implementationType;
    }
  }

  public interface Options extends PipelineOptions {

    @Description("Type of implementation")
    @Default.String("naive")
    String getImplementation();

    void setImplementation(String implementation);

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Output GCP Project for Firestore")
    @Required
    String getOutputGoogleCloudProject();

    void setOutputGoogleCloudProject(String outputGoogleCloudProject);

    @Description("Path to the output Collection in Firestore: /path/to/collection ")
    String getOutputFirestoreCollectionPath();

    void setOutputFirestoreCollectionPath(String outputFirestoreCollectionPath);
  }

}
