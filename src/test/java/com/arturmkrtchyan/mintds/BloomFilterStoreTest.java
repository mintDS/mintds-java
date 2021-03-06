package com.arturmkrtchyan.mintds;

import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class BloomFilterStoreTest extends AbstractStoreTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private BloomFilterStore filterStore;

    @Before
    public void setUp() {
        filterStore = new BloomFilterStore(client);
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
    public void addWithNonExistingKeyShouldThrowException() throws Exception {
        final String time = String.valueOf(System.currentTimeMillis());
        final String filterName = "newfilter" + time;
        expectedException.expectMessage("failure " + filterName + " doesn't exist");
        filterStore.add(filterName, time).get();
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
        final String time = String.valueOf(System.currentTimeMillis());
        final String filterName = "newfilter" + time;
        expectedException.expectMessage("failure " + filterName + " doesn't exist");
        filterStore.contains(filterName, time).get();
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