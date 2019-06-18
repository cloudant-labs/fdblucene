package io.grpc.fdblucene;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;

import com.cloudant.fdblucene.*;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;

public class FDBIndexerServer {
  private static final Logger logger = Logger.getLogger(FDBIndexerServer.class.getName());

  protected static Database DB;
  protected static FDBIndexWriter writer;

  protected Subspace subspace;
  

  private Server server;

  private void start() throws IOException {
    /* The port on which the server should run */
    final int port = 50051;
    final Analyzer analyzer = new StandardAnalyzer();

    subspace = new Subspace(new byte[] { 1, 2, 3 });
    writer = new FDBIndexWriter(DB, subspace, analyzer);
    server = ServerBuilder.forPort(port)
        .addService(new FDBIndexerGrpcImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        FDBIndexerServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
    // this is clean up, but commenting out to see results are written
    // DB.run(txn -> {
    //     txn.clear(subspace.range());
    //     return null;
    // });
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final FDBIndexerServer server = new FDBIndexerServer();
    FDB.selectAPIVersion(600);
    DB = FDB.instance().open();

    server.start();
    server.blockUntilShutdown();
  }

  private static class FDBIndexerGrpcImpl extends FDBIndexerGrpc.FDBIndexerImplBase {

    /**
     * Gets a stream of field value Pairs
     *
     * @param responseObserver an observer to receive the number of fields indexed.
     * @return an observer to receive the FieldValues.
     */
    @Override
    public StreamObserver<FieldValues> indexFields(final StreamObserver<FieldsIndexed> responseObserver) {
      return new StreamObserver<FieldValues>() {
        int fieldCount;

        final long startTime = System.nanoTime();

        @Override
        public void onNext(FieldValues fvs) {
          // Where we would actually index the field
          String field = fvs.getField();
          String value = fvs.getValue();
          logger.info("Field Name: " + field);
          logger.info("Field Value: " + value);

          final Document doc = new Document();
          doc.add(new StringField(field, value, Store.YES));

          try {
            writer.addDocument(doc);
          } catch (IOException e) {
            logger.info("indexing failed: " + e.getMessage());
          }

          fieldCount++;
        }

        @Override
        public void onError(Throwable t) {
          logger.info("indexField cancelled");
        }

        @Override
        public void onCompleted() {
          responseObserver.onNext(FieldsIndexed.newBuilder().setFieldcount(fieldCount).build());
          responseObserver.onCompleted();
        }
      };
    }
  }
}
