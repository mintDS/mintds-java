package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import com.arturmkrtchyan.mintds.protocol.request.Command;
import com.arturmkrtchyan.mintds.protocol.request.DataStructure;
import com.arturmkrtchyan.mintds.protocol.request.DefaultRequest;
import com.arturmkrtchyan.mintds.protocol.request.Request;
import com.arturmkrtchyan.mintds.protocol.response.EnumResponse;
import com.arturmkrtchyan.mintds.protocol.response.FailureResponse;
import com.arturmkrtchyan.mintds.protocol.response.NumericResponse;
import com.arturmkrtchyan.mintds.protocol.response.Response;

import java.util.concurrent.CompletableFuture;

public class HyperLogLogStore {

    private final MintDsClient client;

    public HyperLogLogStore(final MintDsClient client) {
        this.client = client;
    }

    public CompletableFuture<Boolean> create(final String log) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.CREATE)
                .withDataStructure(DataStructure.HyperLogLog)
                .withKey(log)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.SUCCESS ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> exists(final String log) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.EXISTS)
                .withDataStructure(DataStructure.HyperLogLog)
                .withKey(log)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.YES ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> add(final String log, final String value) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.ADD)
                .withDataStructure(DataStructure.HyperLogLog)
                .withKey(log)
                .withValue(value)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.SUCCESS ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Integer> count(final String log) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.COUNT)
                .withDataStructure(DataStructure.HyperLogLog)
                .withKey(log)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return ((NumericResponse)response).getValue().intValue();
        });
    }

    public CompletableFuture<Boolean> drop(final String log) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.DROP)
                .withDataStructure(DataStructure.HyperLogLog)
                .withKey(log)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.SUCCESS ? Boolean.TRUE : Boolean.FALSE;
        });
    }

}
