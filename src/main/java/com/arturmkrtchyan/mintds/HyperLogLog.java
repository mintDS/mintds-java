package com.arturmkrtchyan.mintds;

import java.util.concurrent.CompletableFuture;

public class HyperLogLog {

    public HyperLogLog() {

    }

    public CompletableFuture<Boolean> create(final String log) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> exists(final String log) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> add(final String log, final String value) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Integer> count(final String log) {
        return new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> drop(final String log) {
        return new CompletableFuture<>();
    }

}
