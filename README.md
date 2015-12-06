[![Build Status](https://travis-ci.org/mintDS/mintds-java.svg)](https://travis-ci.org/mintDS/mintds-java)

mintds-java
--------------

A Java client library for mintDS.

### BloomFilter example
```java
MintDsClient client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(100)
                .numberOfThreads(1)
                .build();
BloomFilter filter = new BloomFilter(client);

CompletableFuture<Boolean> createResult = filter.create("myfilter");
CompletableFuture<Boolean> existsResult = filter.exists("myfilter");
CompletableFuture<Boolean> addResult = filter.add("myfilter", "myvalue");
CompletableFuture<Boolean> containsResult = filter.contains("myfilter", "myvalue");
CompletableFuture<Boolean> dropResult = filter.drop("myfilter");

client.close();
```
