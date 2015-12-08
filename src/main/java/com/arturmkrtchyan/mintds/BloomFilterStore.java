package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import com.arturmkrtchyan.mintds.protocol.request.Command;
import com.arturmkrtchyan.mintds.protocol.request.DataStructure;
import com.arturmkrtchyan.mintds.protocol.request.DefaultRequest;
import com.arturmkrtchyan.mintds.protocol.request.Request;
import com.arturmkrtchyan.mintds.protocol.response.EnumResponse;
import com.arturmkrtchyan.mintds.protocol.response.FailureResponse;
import com.arturmkrtchyan.mintds.protocol.response.Response;

import java.util.concurrent.CompletableFuture;

public class BloomFilterStore {

    private final MintDsClient client;

    public BloomFilterStore(final MintDsClient client) {
        this.client = client;
    }

    public CompletableFuture<Boolean> create(final String filter) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.CREATE)
                .withDataStructure(DataStructure.BloomFilter)
                .withKey(filter)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.SUCCESS ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> exists(final String filter) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.EXISTS)
                .withDataStructure(DataStructure.BloomFilter)
                .withKey(filter)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.YES ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> add(final String filter, final String value) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.ADD)
                .withDataStructure(DataStructure.BloomFilter)
                .withKey(filter)
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

    public CompletableFuture<Boolean> contains(final String filter, final String value) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.CONTAINS)
                .withDataStructure(DataStructure.BloomFilter)
                .withKey(filter)
                .withValue(value)
                .build();

        CompletableFuture<Response> future = client.send(request);
        return future.thenApply(response -> {
            if (response instanceof FailureResponse) {
                throw new IllegalStateException(((FailureResponse)response).getMessage());
            }
            return response == EnumResponse.YES ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    public CompletableFuture<Boolean> drop(final String filter) {
        final Request request = new DefaultRequest.Builder()
                .withCommand(Command.DROP)
                .withDataStructure(DataStructure.BloomFilter)
                .withKey(filter)
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
