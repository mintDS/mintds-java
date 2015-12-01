package com.arturmkrtchyan.mintds.client;

import io.netty.util.AttributeKey;

import java.util.concurrent.CompletableFuture;

interface MintDsChannelAttributeKey {
    public static final AttributeKey<CompletableFuture<String>> CALLBACK = AttributeKey.valueOf("callback");
}
