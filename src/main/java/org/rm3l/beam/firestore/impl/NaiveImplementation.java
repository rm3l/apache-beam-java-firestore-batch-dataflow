package org.rm3l.beam.firestore.impl;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.rm3l.beam.WordCount.CountWords;
import org.rm3l.beam.firestore.WordCountToFirestorePipeline.Options;

public class NaiveImplementation extends AbstractImplementation {

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
