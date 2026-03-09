package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
import io.grpc.stub.StreamObserver;

public class ChronicleServiceImpl extends ChronicleServiceGrpc.ChronicleServiceImplBase {

    // TODO
    @Override
    public void appendTx(AppendTxRequest request, StreamObserver<AppendTxResponse> responseObserver) {
        final String cdbId = request.getCdbId();
        final long seqNum = request.getSeqNum();
        final String tx = request.getTx();

        System.out.println(cdbId);
        System.out.println(seqNum);
        System.out.println(tx);

        final AppendTxResponse response = AppendTxResponse.newBuilder()
                .setSuccess(true)
                .setCommittedSeqNum(seqNum)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
