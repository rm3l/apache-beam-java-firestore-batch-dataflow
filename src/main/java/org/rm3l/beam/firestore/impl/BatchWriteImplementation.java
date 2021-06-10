package org.rm3l.beam.firestore.impl;

import com.google.cloud.firestore.Firestore;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.rm3l.beam.WordCount.CountWords;
import org.rm3l.beam.firestore.FirestoreUpdateDoFn;
import org.rm3l.beam.firestore.WordCountToFirestorePipeline.Options;

public class BatchWriteImplementation extends AbstractImplementation {

  @Override
  protected Pipeline doCreatePipeline(final String[] args) {
    final BatchWriteImplementationOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(BatchWriteImplementationOptions.class);
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
                input.apply("Batch write to Firestore", ParDo.of(new FirestoreUpdateDoFn<>(
                    outputGoogleCloudProject, options.getFirestoreMaxBatchSize(),
                    (final Firestore firestoreDb, final KV<String, Long> element) -> {
                      final Map<String, Long> documentData = new HashMap<>();
                      documentData.put("count", element.getValue());

                      firestoreDb.collection(outputFirestoreCollectionPath)
                          .document(element.getKey())
                          .set(documentData);
                    }
                )));
                return PDone.in(input.getPipeline());
              }

            });

    return wordCountToFirestorePipeline;
  }

  public interface BatchWriteImplementationOptions extends Options {

    @Description("Max batch size for Firestore writes")
    @Default.Integer(FirestoreUpdateDoFn.DEFAULT_MAX_BATCH_SIZE)
    int getFirestoreMaxBatchSize();

    void setFirestoreMaxBatchSize(int firestoreMaxBatchSize);
  }
}
