package com.arturmkrtchyan.mintds.client;

import io.netty.util.AttributeKey;

interface MintDsChannelAttributeKey {
    public static final AttributeKey<MintDsCallback> CALLBACK = AttributeKey.valueOf("callback");
}
