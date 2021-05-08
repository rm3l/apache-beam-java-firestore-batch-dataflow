package org.apache.beam.examples.firestore;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.examples.WordCount.CountWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
    naive(NaiveImplementation.class);

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

  static abstract class AbstractImplementation {

    final void run(final String[] args) {
      doCreatePipeline(args).run().waitUntilFinish();
    }

    protected abstract Pipeline doCreatePipeline(final String[] args);
  }

  static class NaiveImplementation extends AbstractImplementation implements java.io.Serializable {

    @Override
    protected Pipeline doCreatePipeline(final String[] args) {
      final Options options =
          PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
      final Pipeline wordCountToFirestorePipeline = Pipeline.create(options);

      final String outputGoogleCloudProject = options.getOutputGoogleCloudProject();
      final String inputFile = options.getInputFile();
      final String outputFirestoreCollectionPath = options
          .getOutputFirestoreCollectionPath() != null ?
          options.getOutputFirestoreCollectionPath() :
          inputFile.substring(inputFile.lastIndexOf("/") + 1, inputFile.length());

      wordCountToFirestorePipeline.apply("ReadLines", TextIO.read().from(inputFile))
          .apply(new CountWords())
          .apply("Write Counts to Firestore",
              new PTransform<PCollection<KV<String, Long>>, PDone>() {
                @Override
                public PDone expand(PCollection<KV<String, Long>> input) {

                  input.apply("Write to Firestore", ParDo.of(new DoFn<KV<String, Long>, Void>() {

                    @ProcessElement
                    public void processElement(@Element KV<String, Long> element,
                        OutputReceiver<Void> out) {

                      try (final Firestore firestore = FirestoreOptions.getDefaultInstance()
                          .toBuilder()
                          .setCredentials(GoogleCredentials.getApplicationDefault())
                          .setProjectId(outputGoogleCloudProject)
                          .build().getService()) {

                        final Map<String, Long> documentData = new HashMap<>();
                        documentData.put("count", element.getValue());

                        firestore.collection(outputFirestoreCollectionPath)
                            .document(element.getKey())
                            .set(documentData).get(1L, TimeUnit.MINUTES);

                      } catch (final Exception e) {
                        logger.warn("Error while writing to Firestore", e);
                        throw new IllegalStateException(e);
                      }

                      out.output(null);
                    }
                  }));

                  return PDone.in(input.getPipeline());
                }
              });

      return wordCountToFirestorePipeline;
    }
  }


}
