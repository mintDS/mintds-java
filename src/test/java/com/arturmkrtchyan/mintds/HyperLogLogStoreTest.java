package com.arturmkrtchyan.mintds;

import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HyperLogLogStoreTest extends AbstractStoreTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static HyperLogLogStore logStore;

    @Before
    public void setUp() {
        logStore = new HyperLogLogStore(client);
    }

    @Test
    public void createWithNonExistingKeyShouldReturnTrue() throws Exception {
        assertTrue(logStore.create("newlog" + System.currentTimeMillis()).get());
    }

    @Test
    public void createWithExistingKeyShouldReturnFalse() throws Exception {
        final long time = System.currentTimeMillis();
        logStore.create("newlog" + time).get();
        assertFalse(logStore.create("newlog" + time).get());
    }

    @Test
    public void existsWithExistingKeyShouldReturnTrue() throws Exception {
        final long time = System.currentTimeMillis();
        logStore.create("newlog" + time).get();
        assertTrue(logStore.exists("newlog" + time).get());
    }

    @Test
    public void existsWithNonExistingKeyShouldReturnFalse() throws Exception {
        assertFalse(logStore.exists("newlog" + System.currentTimeMillis()).get());
    }

    @Test
    public void deopWithExistingKeyShouldReturnTrue() throws Exception {
        final long time = System.currentTimeMillis();
        logStore.create("newlog" + time).get();
        assertTrue(logStore.drop("newlog" + time).get());
    }

    @Test
    public void dropWithNonExistingKeyShouldReturnFalse() throws Exception {
        assertFalse(logStore.drop("newlog" + System.currentTimeMillis()).get());
    }

    @Test
    public void addWithNonExistingKeyShouldThrowException() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        final String logName = "newlog" + time;
        expectedException.expectMessage("failure " + logName + " doesn't exist");
        logStore.add(logName, time).get();
    }

    @Test
    public void addWithExistingKeyShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        logStore.create("newlog" + time).get();
        assertTrue(logStore.add("newlog" + time, time).get());
    }

    @Test
    public void addWithExistingKeyAndExistingValueShouldReturnTrue() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        logStore.create("newlog" + time).get();
        logStore.add("newlog" + time, time).get();
        assertTrue(logStore.add("newlog" + time, time).get());
    }

    @Test
    public void countWithNonExistingKeyShouldThrowException() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        final String logName = "newlog" + time;
        expectedException.expectMessage("failure " + logName + " doesn't exist");
        logStore.count(logName).get();
    }

    @Test
    public void countWithExistingKeyAndOneDistinctValueShouldReturnOne() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        logStore.create("newlog" + time).get();
        logStore.add("newlog" + time, time).get();
        logStore.add("newlog" + time, time).get();
        assertEquals(Integer.valueOf(1), logStore.count("newlog" + time).get());
    }

    @Test
    public void countWithExistingKeyAndTwoDistinctValueShouldReturnTwo() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        logStore.create("newlog" + time).get();
        logStore.add("newlog" + time, time + 1).get();
        logStore.add("newlog" + time, time + 2).get();
        assertEquals(Integer.valueOf(2), logStore.count("newlog" + time).get());
    }
}