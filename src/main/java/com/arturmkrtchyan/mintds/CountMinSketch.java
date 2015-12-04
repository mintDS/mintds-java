package com.arturmkrtchyan.mintds;

import java.util.concurrent.CompletableFuture;

public class CountMinSketch {

    public CompletableFuture<Boolean> create(final String sketch) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> exists(final String sketch) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> add(final String sketch, final String value) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Integer> count(final String sketch) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> drop(final String sketch) {
        return new CompletableFuture<>();
    }

}
