package org.rm3l.beam.firestore;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirestoreUpdateDoFn<T> extends DoFn<T, Void> {

  public static final int DEFAULT_MAX_BATCH_SIZE = 500;

  private final Logger logger = LoggerFactory.getLogger(FirestoreUpdateDoFn.class);

  private final String outputFirestoreProjectId;
  private final long maxBatchSize;
  private final List<T> elementsBatch = new LinkedList<>();
  private final InputToDocumentRefUpdater<T> inputToDocumentRefUpdaterFunction;

  private Firestore firestoreDb;

  public FirestoreUpdateDoFn(final String outputFirestoreProjectId, final long maxBatchSize,
      final InputToDocumentRefUpdater<T> inputToDocumentRefUpdaterFunction) {
    this.outputFirestoreProjectId = outputFirestoreProjectId;
    this.maxBatchSize = maxBatchSize;
    this.inputToDocumentRefUpdaterFunction = inputToDocumentRefUpdaterFunction;
  }

  @StartBundle
  public void startBundle() throws Exception {
    logger.debug("Starting processing bundle...");
    this.firestoreDb = FirestoreOptions.getDefaultInstance()
        .toBuilder()
        .setCredentials(GoogleCredentials.getApplicationDefault())
        .setProjectId(outputFirestoreProjectId)
        .build().getService();
  }

  @ProcessElement
  public void processElement(final ProcessContext context)
      throws ExecutionException, InterruptedException {
    final T element = context.element();
    logger.debug("Adding element to batch: {}", element);
    this.elementsBatch.add(element);
    if (this.elementsBatch.size() >= this.maxBatchSize) {
      this.flushUpdates();
    }
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    logger.debug("Finishing processing bundle...");
    this.flushUpdates();
    if (this.firestoreDb != null) {
      this.firestoreDb.close();
    }
  }

  @Teardown
  public void teardown() throws Exception {
    try {
      if (this.firestoreDb != null) {
        this.firestoreDb.close();
      }
    } catch (final Exception e) {
      logger.warn("Error in teardown method", e);
    }
  }

  private void flushUpdates() throws ExecutionException, InterruptedException {
    if (elementsBatch.isEmpty()) {
      return;
    }
    logger.debug("Flushing {} operations to Firestore...", elementsBatch.size());
    final long start = System.nanoTime();
    final WriteBatch writeBatch = firestoreDb.batch();
    final List<T> processed = new ArrayList<>();
    elementsBatch.forEach(element -> {
      inputToDocumentRefUpdaterFunction
          .updateDocumentInFirestore(this.firestoreDb, element);
      processed.add(element);
    });
    writeBatch.commit().get();
    logger.info("... Committed {} operations to Firestore in {} ms...", processed.size(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    this.elementsBatch.removeAll(processed);
  }

  @FunctionalInterface
  public interface InputToDocumentRefUpdater<T> extends Serializable {

    void updateDocumentInFirestore(Firestore firestoreDb, T input);
  }

}
