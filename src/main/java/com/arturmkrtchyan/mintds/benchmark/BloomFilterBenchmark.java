package com.arturmkrtchyan.mintds.benchmark;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import com.arturmkrtchyan.mintds.protocol.request.Command;
import com.arturmkrtchyan.mintds.protocol.request.DataStructure;
import com.arturmkrtchyan.mintds.protocol.request.DefaultRequest;
import com.arturmkrtchyan.mintds.protocol.response.Response;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class BloomFilterBenchmark {

    public static void main(String[] args) throws Exception {

        final String host = System.getProperty("host", "localhost");
        final int port = Integer.valueOf(System.getProperty("port", "7657"));
        final int threads = Integer.valueOf(System.getProperty("threads", "1"));
        final int connections = Integer.valueOf(System.getProperty("connections", "10"));
        final int messages = Integer.valueOf(System.getProperty("messages", "100000"));
        final int iterations = Integer.valueOf(System.getProperty("iterations", "10"));


        System.out.println("--------------------->Options<-----------------");
        System.out.println("host: " + host);
        System.out.println("port: " + port);
        System.out.println("threads: " + threads);
        System.out.println("connections: " + connections);
        System.out.println("messages: " + messages);
        System.out.println("iterations: " + iterations);
        System.out.println("-----------------------------------------------");

        MintDsClient client = new MintDsClient.Builder()
                .host(host)
                .port(port)
                .numberOfConnections(connections)
                .numberOfThreads(threads)
                .build();

        System.out.println("----------------------------------->>BloomFilter<<-----------------------------------");

        IntStream.range(0, iterations).forEach(iteration -> {
            System.out.println("---------------------Iteration ->" + iteration + "---------------------");
            try {
                final String filterName = "filter-" + iteration;
                client.send(new DefaultRequest.Builder()
                        .withCommand(Command.CREATE)
                        .withDataStructure(DataStructure.BloomFilter)
                        .withKey(filterName)
                        .build()).get(2, TimeUnit.SECONDS);
                System.out.println("Creating Filter: " + filterName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Asynchronously sending " + (messages / 1000) + "K messages.");

            CompletableFuture[] futures = new CompletableFuture[messages];

            long start = System.currentTimeMillis();

            IntStream.range(0, messages).forEach(index -> {
                try {
                    CompletableFuture<Response> future = client.send(new DefaultRequest.Builder()
                            .withCommand(Command.ADD)
                            .withDataStructure(DataStructure.BloomFilter)
                            .withKey("test")
                            .withValue("mytestvalue" + index)
                            .build());
                    futures[index] = future;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            System.out.println("It took " + (System.currentTimeMillis() - start) +
                    "ms to asynchronously send " + (messages/1000) + "K messages.");

            System.out.println("Waiting to get all responses from server.");
            CompletableFuture f = CompletableFuture.allOf(futures);
            try {
                f.get(300, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("Server handles " + (messages/(System.currentTimeMillis() - start)) +
                    "K messages per second");

        });

        client.close();;
    }

}
