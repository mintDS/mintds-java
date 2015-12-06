package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import com.arturmkrtchyan.mintds.protocol.request.Command;
import com.arturmkrtchyan.mintds.protocol.request.DataStructure;
import com.arturmkrtchyan.mintds.protocol.request.DefaultRequest;
import com.arturmkrtchyan.mintds.protocol.request.Request;
import com.arturmkrtchyan.mintds.protocol.response.EnumResponse;
import com.arturmkrtchyan.mintds.protocol.response.FailureResponse;
import com.arturmkrtchyan.mintds.protocol.response.Response;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class BloomFilter {

    private final MintDsClient client;

    public BloomFilter(final MintDsClient client) {
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
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> add(final String filter, final String value) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> contains(final String filter, final String value) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> drop(final String filter) {
        return new CompletableFuture<>();
    }

}
