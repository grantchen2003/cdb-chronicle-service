package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ChronicleServiceImplTest {
    private static final int NUM_EXECUTOR_THREADS = 50;
    private ChronicleServiceImpl service;
    private ExecutorService executor;
    private ChronicleLogProducerStub logProducer;

    @BeforeEach
    void setUp() {
        final Map<String, Long> cdbIdToSn = new HashMap<>();
        logProducer = new ChronicleLogProducerStub();
        service = new ChronicleServiceImpl(cdbIdToSn, logProducer);
        executor = Executors.newFixedThreadPool(NUM_EXECUTOR_THREADS);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void testAppendTx_ConcurrentRequestsToSameCdbOnlyOneSucceeds() throws InterruptedException {
        final int numConcurrentRequests = NUM_EXECUTOR_THREADS;

        for (int i = 0; i < 100; i++) {
            final String cdbId = "cdb_" + i;
            final long targetSn = 1L;

            final AppendTxRequest request = AppendTxRequest.newBuilder()
                    .setCdbId(cdbId)
                    .setSeqNum(targetSn)
                    .build();

            final CountDownLatch startGate = new CountDownLatch(1);
            final CountDownLatch finishGate = new CountDownLatch(numConcurrentRequests);

            final List<AppendTxResponseStub> stubs = new ArrayList<>();

            for (int j = 0; j < numConcurrentRequests; j++) {
                final AppendTxResponseStub stub = new AppendTxResponseStub(finishGate);
                stubs.add(stub);

                executor.submit(() -> {
                    try {
                        startGate.await();
                        service.appendTx(request, stub);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            startGate.countDown();

            if (!finishGate.await(10, TimeUnit.SECONDS)) {
                Assertions.fail("High contention test timed out at iteration " + i);
            }

            final List<AppendTxResponseStub> successStubs = stubs.stream()
                    .filter(AppendTxResponseStub::isSuccess)
                    .toList();

            final List<AppendTxResponseStub> failureStubs = stubs.stream()
                    .filter(s -> !s.isSuccess())
                    .toList();

            Assertions.assertEquals(1, successStubs.size(), "Exactly one request should succeed for " + cdbId);
            final AppendTxResponse success = successStubs.getFirst().getResponse();
            Assertions.assertEquals(targetSn, success.getCommittedSeqNum(), "CommittedSeqNum should match targetSn");

            Assertions.assertEquals(numConcurrentRequests - 1, failureStubs.size());
            for (final AppendTxResponseStub failureStub : failureStubs) {
                Assertions.assertEquals(Status.ABORTED.getCode(), failureStub.getError().getStatus().getCode());
            }
        }
    }

    @Test
    void testAppendTx_ConcurrentRequestsToTwoDifferentCdbBothSucceed() throws InterruptedException {
        final String cdb1 = "cdb1";
        final String cdb2 = "cdb2";

        final AppendTxRequest req1 = AppendTxRequest.newBuilder()
                .setCdbId(cdb1)
                .setSeqNum(1)
                .build();

        final AppendTxRequest req2 = AppendTxRequest.newBuilder()
                .setCdbId(cdb2)
                .setSeqNum(1)
                .build();

        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch finishGate = new CountDownLatch(2);

        final AppendTxResponseStub stub1 = new AppendTxResponseStub(finishGate);
        final AppendTxResponseStub stub2 = new AppendTxResponseStub(finishGate);

        executor.submit(() -> {
            try {
                startGate.await();
                service.appendTx(req1, stub1);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });

        executor.submit(() -> {
            try {
                startGate.await();
                service.appendTx(req2, stub2);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });

        startGate.countDown();

        if (!finishGate.await(5, TimeUnit.SECONDS)) {
            Assertions.fail("Different ID test timed out");
        }

        Assertions.assertTrue(stub1.isSuccess(), "cdb1 should succeed independently of cdb2");
        Assertions.assertEquals(1L, stub1.getResponse().getCommittedSeqNum(), "CommittedSeqNum for cdb1 should match the requested SN");

        Assertions.assertTrue(stub2.isSuccess(), "cdb2 should succeed independently of cdb1");
        Assertions.assertEquals(1L, stub2.getResponse().getCommittedSeqNum(), "CommittedSeqNum for cdb2 should match the requested SN");
    }

    @Test
    void testAppendTx_PersistenceFailureDoesNotIncrementSequence() {
        final String cdbId = "test-cdb";

        logProducer.setShouldFail(true);

        final AppendTxRequest failReq = AppendTxRequest.newBuilder()
                .setCdbId(cdbId)
                .setSeqNum(1)
                .setTx("data-1")
                .build();

        final AppendTxResponseStub failStub = new AppendTxResponseStub(new CountDownLatch(1));
        service.appendTx(failReq, failStub);

        Assertions.assertFalse(failStub.isSuccess(), "Should fail when producer fails");
        Assertions.assertEquals(Status.INTERNAL.getCode(), failStub.getError().getStatus().getCode());

        logProducer.setShouldFail(false);

        final AppendTxRequest successReq = AppendTxRequest.newBuilder()
                .setCdbId(cdbId)
                .setSeqNum(1)
                .setTx("data-1-retry")
                .build();

        final AppendTxResponseStub successStub = new AppendTxResponseStub(new CountDownLatch(1));
        service.appendTx(successReq, successStub);

        Assertions.assertTrue(successStub.isSuccess(), "Should succeed now that Kafka is up");
        Assertions.assertEquals(1L, successStub.getResponse().getCommittedSeqNum(), "SN 1 should now be committed");
    }
}