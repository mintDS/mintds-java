[![Build Status](https://travis-ci.org/mintDS/mintds-java.svg)](https://travis-ci.org/mintDS/mintds-java)

mintds-java
--------------

Asynchronous Java client library for mintDS.

### BloomFilter example
```java
MintDsClient client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(100)
                .numberOfThreads(1)
                .build();
BloomFilterStore filterStore = new BloomFilterStore(client);

CompletableFuture<Boolean> createResult = filterStore.create("myfilter");
CompletableFuture<Boolean> existsResult = filterStore.exists("myfilter");
CompletableFuture<Boolean> addResult = filterStore.add("myfilter", "myvalue");
CompletableFuture<Boolean> containsResult = filterStore.contains("myfilter", "myvalue");
CompletableFuture<Boolean> dropResult = filterStore.drop("myfilter");

client.close();
```
