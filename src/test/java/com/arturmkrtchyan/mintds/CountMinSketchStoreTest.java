package com.arturmkrtchyan.mintds;

import com.arturmkrtchyan.mintds.client.MintDsClient;
import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class CountMinSketchStoreTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static CountMinSketchStore sketchStore;
    private static MintDsClient client;

    @BeforeClass
    public static void setUp() {
        client = new MintDsClient.Builder()
                .host("localhost")
                .port(7657)
                .numberOfConnections(100)
                .numberOfThreads(1)
                .build();
        sketchStore = new CountMinSketchStore(client);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void createWithNonExistingKeyShouldReturnTrue() throws Exception {
        assertTrue(sketchStore.create("newsketch" + System.currentTimeMillis()).get());
    }

    @Test
    //@Ignore
    public void createWithExistingKeyShouldReturnFalse() throws Exception {
        final long time = System.currentTimeMillis();
        sketchStore.create("newsketch" + time).get();
        assertFalse(sketchStore.create("newsketch" + time).get());
    }

    @Test
    public void existsWithExistingKeyShouldReturnTrue() throws Exception {
        final long time = System.currentTimeMillis();
        sketchStore.create("newsketch" + time).get();
        assertTrue(sketchStore.exists("newsketch" + time).get());
    }

    @Test
    public void existsWithNonExistingKeyShouldReturnFalse() throws Exception {
        assertFalse(sketchStore.exists("newsketch" + System.currentTimeMillis()).get());
    }

    @Test
    public void deopWithExistingKeyShouldReturnTrue() throws Exception {
        final long time = System.currentTimeMillis();
        sketchStore.create("newsketch" + time).get();
        assertTrue(sketchStore.drop("newsketch" + time).get());
    }

    @Test
    public void dropWithNonExistingKeyShouldReturnFalse() throws Exception {
        assertFalse(sketchStore.drop("newsketch" + System.currentTimeMillis()).get());
    }

    @Test
    public void addWithNonExistingKeyShouldThrowException() throws Exception {
        expectedException.expectMessage("failure filter doesn't exist");
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.add("newsketch" + time, time).get();
    }

    @Test
    public void addWithExistingKeyShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.create("newsketch" + time).get();
        assertTrue(sketchStore.add("newsketch" + time, time).get());
    }

    @Test
    public void addWithExistingKeyAndExistingValueShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.create("newsketch" + time).get();
        sketchStore.add("newsketch" + time, time).get();
        assertTrue(sketchStore.add("newsketch" + time, time).get());
    }

    @Test
    public void countWithNonExistingKeyShouldThrowException() throws Exception {
        expectedException.expectMessage("failure filter doesn't exist");
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.count("newsketch" + time, time).get();
    }

    @Test
    public void countWithExistingKeyAndOneValueShouldReturnOne() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.create("newsketch" + time).get();
        sketchStore.add("newsketch" + time, time).get();
        assertEquals(Integer.valueOf(1), sketchStore.count("newsketch" + time, time).get());
    }

    @Test
    public void countWithExistingKeyAndTwoValueShouldReturnTwo() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.create("newsketch" + time).get();
        sketchStore.add("newsketch" + time, time).get();
        sketchStore.add("newsketch" + time, time).get();
        assertEquals(Integer.valueOf(2), sketchStore.count("newsketch" + time, time).get());
    }

    @Test
    public void countWithExistingKeyAndNonExistingValueShouldReturnZero() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        sketchStore.create("newsketch" + time).get();
        sketchStore.add("newsketch" + time, time).get();
        assertEquals(Integer.valueOf(0), sketchStore.count("newsketch" + time, time + time).get());
    }
}