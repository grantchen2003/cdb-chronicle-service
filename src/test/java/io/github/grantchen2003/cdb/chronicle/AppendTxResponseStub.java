package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

public class AppendTxResponseStub implements StreamObserver<AppendTxResponse> {
    private AppendTxResponse response;
    private final CountDownLatch latch;

    public AppendTxResponseStub(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onNext(AppendTxResponse value) {
        this.response = value;
    }

    @Override
    public void onError(Throwable t) {
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public AppendTxResponse getResponse() {
        return response;
    }
}
