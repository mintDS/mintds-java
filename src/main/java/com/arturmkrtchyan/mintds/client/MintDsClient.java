package com.arturmkrtchyan.mintds.client;

import com.arturmkrtchyan.mintds.protocol.request.Command;
import com.arturmkrtchyan.mintds.protocol.request.DataStructure;
import com.arturmkrtchyan.mintds.protocol.request.DefaultRequest;
import com.arturmkrtchyan.mintds.protocol.request.Request;
import com.arturmkrtchyan.mintds.protocol.response.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MintDsClient implements AutoCloseable {

    protected static final AttributeKey<CompletableFuture<Response>> FUTURE = AttributeKey.valueOf("future");

    public static final int DEFAULT_THREADS = 1;
    public static final int DEFAULT_CONNECTIONS = 16;

    private String host;
    private int port;
    private int numberOfThreads;
    private int numberOfConnections;
    private EventLoopGroup eventLoopGroup;
    private ChannelPool channelPool;

    private MintDsClient() {
        numberOfThreads = DEFAULT_THREADS;
        numberOfConnections = DEFAULT_CONNECTIONS;
    }

    private void connect() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(numberOfThreads);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
        bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);

        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(host, port);

        MintDsClientInitializer initializer = new MintDsClientInitializer(() -> new MintDsClientHandler(channelPool));


        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) {
                initializer.initChannel(ch);
            }
        }, numberOfConnections);


    }

    @Override
    public void close() throws Exception {
        disconnect();
    }

    private void disconnect() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    public CompletableFuture<Response> send(final Request request) {
        // Sends the message to the server.
        CompletableFuture<Response> future = new CompletableFuture<>();

        Future<Channel> f = channelPool.acquire();
        f.addListener(new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> f) {
                if (f.isSuccess()) {
                    Channel channel = f.getNow();
                    CompletableFuture<Response> previous = channel.attr(FUTURE).getAndSet(future);
                    if (previous != null) {
                        System.err.println("Internal error, completion handler should have been null");
                    }
                    try {
                        channel.writeAndFlush(request.toString() + "\r\n", channel.voidPromise());
                    } catch (Exception e) {
                        CompletableFuture<Response> current = channel.attr(FUTURE).getAndRemove();
                        channelPool.release(channel);
                        current.completeExceptionally(e);
                    }
                }
            }
        });
        return future;
    }

    public static void main(String[] args) throws Exception {

        MintDsClient client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(1024)
                .numberOfThreads(1)
                .build();

        CompletableFuture<Response> future = client.send(new DefaultRequest.Builder()
                .withCommand(Command.CREATE)
                .withDataStructure(DataStructure.BloomFilter)
                .withKey("test")
                .build());
        System.out.println("aaaa");
        System.out.println(future.get());

        List<CompletableFuture<Response>> futures =  Collections.synchronizedList(new LinkedList<>());

        long start = System.currentTimeMillis();

        IntStream.range(0, 10000).parallel().forEach(value -> {
            try {
                CompletableFuture<Response> f = client.send(new DefaultRequest.Builder()
                        .withCommand(Command.ADD)
                        .withDataStructure(DataStructure.BloomFilter)
                        .withKey("test")
                        .withValue("mytestvalue" + value)
                        .build());
                futures.add(f);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        System.out.println(System.currentTimeMillis() - start);

        CompletableFuture f = CompletableFuture.allOf(futures.toArray(new CompletableFuture[10000]));
        f.get(30, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();

        System.out.println(end - start);
        client.disconnect();

    }

    public static class Builder {

        private MintDsClient client = new MintDsClient();

        public Builder host(final String host) {
            client.host = host;
            return this;
        }

        public Builder port(final int port) {
            client.port = port;
            return this;
        }

        public Builder numberOfThreads(final int threads) {
            client.numberOfThreads = threads;
            return this;
        }

        public Builder numberOfConnections(final int connections) {
            client.numberOfConnections = connections;
            return this;
        }

        public MintDsClient build() {
            try {
                client.connect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return client;
        }

    }

}
