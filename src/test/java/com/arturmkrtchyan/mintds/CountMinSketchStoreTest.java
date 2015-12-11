package com.arturmkrtchyan.mintds;

import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CountMinSketchStoreTest extends AbstractStoreTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static CountMinSketchStore sketchStore;

    @Before
    public void setUp() {
        sketchStore = new CountMinSketchStore(client);
    }

    @Test
    public void createWithNonExistingKeyShouldReturnTrue() throws Exception {
        assertTrue(sketchStore.create("newsketch" + System.currentTimeMillis()).get());
    }

    @Test
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
        final String time = String.valueOf(System.currentTimeMillis());
        final String sketchName = "newsketch" + time;
        expectedException.expectMessage("failure " + sketchName + " doesn't exist");
        sketchStore.add(sketchName, time).get();
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
        final String time = String.valueOf(System.currentTimeMillis());
        final String sketchName = "newsketch" + time;
        expectedException.expectMessage("failure " + sketchName + " doesn't exist");
        sketchStore.count(sketchName, time).get();
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