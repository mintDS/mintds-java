package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

public class BloomFilterTest {

    private BloomFilter filter;
    private MintDsClient client;

    @Before
    public void setUp() {
        client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(100)
                .numberOfThreads(1)
                .build();
        filter = new BloomFilter(client);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }

    @Test
    @Ignore
    public void myTest() throws Exception {
        CompletableFuture<Boolean> future = filter.create("newfilter");
        System.out.println(future.get());
    }

}