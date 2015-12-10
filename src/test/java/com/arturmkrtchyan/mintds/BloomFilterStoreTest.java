package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

@Ignore
public class BloomFilterStoreTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static BloomFilterStore filterStore;
    private static MintDsClient client;

    @BeforeClass
    public static void setUp() {
        client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(100)
                .numberOfThreads(1)
                .build();
        filterStore = new BloomFilterStore(client);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void createWithNonExistingKeyShouldReturnTrue() throws Exception {
        assertTrue(filterStore.create("newfilter" + System.currentTimeMillis()).get());
    }

    @Test
    //@Ignore
    public void createWithExistingKeyShouldReturnFalse() throws Exception {
        final long time = System.currentTimeMillis();
        filterStore.create("newfilter" + time).get();
        assertFalse(filterStore.create("newfilter" + time).get());
    }

    @Test
    public void existsWithExistingKeyShouldReturnTrue() throws Exception {
        final long time = System.currentTimeMillis();
        filterStore.create("newfilter" + time).get();
        assertTrue(filterStore.exists("newfilter" + time).get());
    }

    @Test
    public void existsWithNonExistingKeyShouldReturnFalse() throws Exception {
        assertFalse(filterStore.exists("newfilter" + System.currentTimeMillis()).get());
    }

    @Test
    public void deopWithExistingKeyShouldReturnTrue() throws Exception {
        final long time = System.currentTimeMillis();
        filterStore.create("newfilter" + time).get();
        assertTrue(filterStore.drop("newfilter" + time).get());
    }

    @Test
    public void dropWithNonExistingKeyShouldReturnFalse() throws Exception {
        assertFalse(filterStore.drop("newfilter" + System.currentTimeMillis()).get());
    }

    @Test
    //@Ignore
    public void addWithNonExistingKeyShouldThrowException() throws Exception {
        expectedException.expectMessage("failure filter doesn't exist");
        final String time = String.valueOf(System.currentTimeMillis());
        filterStore.add("newfilter" + time, time).get();
    }

    @Test
    public void addWithExistingKeyShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        filterStore.create("newfilter" + time).get();
        assertTrue(filterStore.add("newfilter" + time, time).get());
    }

    @Test
    public void addWithExistingKeyAndExistingValueShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        filterStore.create("newfilter" + time).get();
        filterStore.add("newfilter" + time, time).get();
        assertTrue(filterStore.add("newfilter" + time, time).get());
    }

    @Test
    public void containsWithNonExistingKeyShouldThrowException() throws Exception {
        expectedException.expectMessage("failure filter doesn't exist");
        final String time = String.valueOf(System.currentTimeMillis());
        filterStore.contains("newfilter" + time, time).get();
    }

    @Test
    public void containsWithExistingKeyAndValueShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        filterStore.create("newfilter" + time).get();
        filterStore.add("newfilter" + time, time).get();
        assertTrue(filterStore.contains("newfilter" + time, time).get());
    }

    @Test
    public void containsWithExistingKeyAndNonExistingValueShouldReturnFalse() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        filterStore.create("newfilter" + time).get();
        filterStore.add("newfilter" + time, time).get();
        assertFalse(filterStore.contains("newfilter" + time, time + time).get());
    }
}