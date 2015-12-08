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

public class CountMinSketchStore {

    private final MintDsClient client;

    public CountMinSketchStore(final MintDsClient client) {
        this.client = client;
    }


    public CompletableFuture<Boolean> create(final String sketch) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.CREATE)
                .withDataStructure(DataStructure.CountMinSketch)
                .withKey(sketch)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.SUCCESS ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> exists(final String sketch) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.EXISTS)
                .withDataStructure(DataStructure.CountMinSketch)
                .withKey(sketch)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.YES ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> add(final String sketch, final String value) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.ADD)
                .withDataStructure(DataStructure.CountMinSketch)
                .withKey(sketch)
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

    public CompletableFuture<Integer> count(final String sketch, final String value) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.COUNT)
                .withDataStructure(DataStructure.CountMinSketch)
                .withKey(sketch)
                .withValue(value)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return ((NumericResponse)response).getValue().intValue();
        });
    }

    public CompletableFuture<Boolean> drop(final String sketch) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.DROP)
                .withDataStructure(DataStructure.CountMinSketch)
                .withKey(sketch)
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
