package com.arturmkrtchyan.mintds.client;

import com.arturmkrtchyan.mintds.protocol.response.Response;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

class MintDsClientHandler extends SimpleChannelInboundHandler<String> {

    private final static Logger logger = LoggerFactory.getLogger(MintDsClientHandler.class);

    private final ChannelPool channelPool;

    public MintDsClientHandler(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        Attribute<CompletableFuture<Response>> futureAttribute = ctx.channel().attr(MintDsClient.FUTURE);
        CompletableFuture<Response> future = futureAttribute.getAndRemove();

        channelPool.release(ctx.channel());
        future.complete(Response.fromString(msg));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Closing connection due to exception.", cause);
        Attribute<CompletableFuture<Response>> futureAttribute = ctx.channel().attr(MintDsClient.FUTURE);
        CompletableFuture<Response> future = futureAttribute.getAndRemove();

        channelPool.release(ctx.channel());
        ctx.close();
        future.completeExceptionally(cause);
    }

}
