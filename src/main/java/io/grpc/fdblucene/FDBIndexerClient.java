package io.grpc.fdblucene;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A client that sends field value pairs to {@link FDBIndexerServer}.
 */
public class FDBIndexerClient {
  private static final Logger logger = Logger.getLogger(FDBIndexerClient.class.getName());

  private final ManagedChannel channel;
  private final FDBIndexerGrpc.FDBIndexerBlockingStub blockingStub;
  private final FDBIndexerGrpc.FDBIndexerStub asyncStub;

  /** Construct client connecting to FDBIndexer server at {@code host:port}. */
  public FDBIndexerClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        .usePlaintext()
        .build());
  }

  /** Construct client for accessing FDBIndexer server using the existing channel. */
  FDBIndexerClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = FDBIndexerGrpc.newBlockingStub(channel);
    asyncStub = FDBIndexerGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

   /* Async indexing that sends field values
   */
  public void indexFields(List<FieldValues> fvs) throws InterruptedException {
    logger.info("*** SendFieldValues");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<FieldsIndexed> responseObserver = new StreamObserver<FieldsIndexed>() {
      @Override
      public void onNext(FieldsIndexed fieldsIndexed) {
        logger.info("FieldsIndexed: " + fieldsIndexed.getFieldcount());
      }

      @Override
      public void onError(Throwable t) {
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        logger.info("Finished Indexing");
        finishLatch.countDown();
      }
    };

    StreamObserver<FieldValues> requestObserver = asyncStub.indexFields(responseObserver);
    try {
      for (FieldValues fv : fvs) {
        requestObserver.onNext(fv);
        // Sleep for a bit before sending the next one.
        Thread.sleep(500);
        if (finishLatch.getCount() == 0) {
          // RPC completed or errored before we finished sending.
          // Sending further requests won't error, but they will just be thrown away.
          return;
        }
      }
    } catch (RuntimeException e) {
      // Cancel RPC
      requestObserver.onError(e);
      throw e;
    }
    // Mark the end of requests
    requestObserver.onCompleted();

    // Receiving happens asynchronously
    if (!finishLatch.await(1, TimeUnit.MINUTES)) {
      logger.info("Indexing did not finish within 1 minutes");
    }
  }


  public static void main(String[] args) throws Exception {
    FDBIndexerClient client = new FDBIndexerClient("localhost", 50051);
    ArrayList<FieldValues> fvs = new ArrayList();
    FieldValues fv = FieldValues.newBuilder().setField("foo").setValue("bar").build();
    fvs.add(fv);
    try {
      client.indexFields(fvs);
    } finally {
      client.shutdown();
    }
  }
}
