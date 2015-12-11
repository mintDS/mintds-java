package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import com.arturmkrtchyan.mintds.server.MintDsServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AbstractStoreTest {

    private static MintDsServer server;
    protected static MintDsClient client;
    private static ExecutorService executor;

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 4445;

    @BeforeClass
    public static void beforeClass() throws Exception {
        startServer();
        client = new MintDsClient.Builder()
                .host(DEFAULT_HOST)
                .port(DEFAULT_PORT)
                .numberOfThreads(1)
                .numberOfConnections(100)
                .build();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        client.close();
        stopServer();
    }

    protected static void startServer() throws Exception {
        System.setProperty("port", String.valueOf(DEFAULT_PORT));
        executor = Executors.newFixedThreadPool(1);
        server = new MintDsServer();
        CompletableFuture.runAsync(server::start, executor);
        Thread.sleep(3000);
    }

    protected static void stopServer() {
        server.stop();
        executor.shutdown();
    }

}
