package com.arturmkrtchyan.mintds.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MintDsClientHandler extends SimpleChannelInboundHandler<String> {

    private final static Logger logger = LoggerFactory.getLogger(MintDsClientHandler.class);

    private final ChannelPool channelPool;

    public MintDsClientHandler(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        Attribute<MintDsCallback> callbackAttribute = ctx.channel().attr(MintDsChannelAttributeKey.CALLBACK);
        MintDsCallback callback = callbackAttribute.getAndRemove();
        //System.out.println(ctx.channel().toString() + Thread.currentThread().getName() + " " + msg);
        channelPool.release(ctx.channel());
        callback.onSuccess(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Closing connection due to exception.", cause);
        Attribute<MintDsCallback> callbackAttribute = ctx.attr(MintDsChannelAttributeKey.CALLBACK);
        MintDsCallback callback = callbackAttribute.getAndRemove();

        channelPool.release(ctx.channel());
        ctx.close();
        callback.onFailure(cause);
    }

}
