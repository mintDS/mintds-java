package com.arturmkrtchyan.mintds.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Random;
import java.util.stream.IntStream;

public class MintDsClient {

    private static final int DEFAULT_THREADS = 1;
    private static final int DEFAULT_CONNECTIONS = 16;

    private String host;
    private int port;
    private int numberOfThreads;
    private int numberOfConnections;
    private Random random;

    private EventLoopGroup eventLoopGroup;
    private ChannelPool channelPool;

    private MintDsClient() {}

    private void connect() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(numberOfThreads);
        Bootstrap bootstrap = new Bootstrap();
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

    public void disconnect() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    public void send(final String message) throws Exception {
        // Sends the message to the server.
        Channel channel = randomChannel();
        channel.writeAndFlush(message + "\r\n").sync();
        channelPool.release(channel);
    }

    protected Channel randomChannel() {
        /*
        Future<Channel> f = channelPool.acquire();
        f.addListener(new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> f) {
                if (f.isSuccess()) {
                    Channel ch = f.getNow();
                    // Do somethings
                    // ...
                    // ...

                    // Release back to pool
                    channelPool.release(ch);
                }
            }
        });
        */
        try {
            return channelPool.acquire().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws Exception {

        MintDsClient client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(128)
                .numberOfThreads(4)
                .build();

        long start = System.currentTimeMillis();

        client.send("create bloomfilter test");

        IntStream.range(0, 1000).parallel().forEach(value -> {
            try {
                client.send("add bloomfilter test mytestvalue"+value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

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
            client.random = new Random();

            try {
                client.connect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return client;
        }

    }

}
