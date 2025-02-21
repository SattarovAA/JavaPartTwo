package org.jpt.buffer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConcurrentCircularBufferTest {
    private ConcurrentCircularBuffer<String> buffer;

    @Test
    @DisplayName("offer() test with non-full buffer.")
    void givenNonFullBufferWhenOfferThenTrue() {
        buffer = new ConcurrentCircularBuffer<>(1);

        boolean actual = buffer.offer("1");

        assertTrue(actual);
        assertEquals(buffer.getSize(), 1);
    }

    @Test
    @DisplayName("offer() test with full buffer.")
    void givenFullBufferWhenOfferThenFalse() {
        buffer = new ConcurrentCircularBuffer<>(new String[]{"1"});

        boolean actual = buffer.offer("1");

        assertFalse(actual);
        assertEquals(buffer.getSize(), 1);
    }

    @Test
    @DisplayName("poll() test with empty buffer.")
    void givenEmptyBufferWhenPollThenNull() {
        buffer = new ConcurrentCircularBuffer<>();

        boolean actual = buffer.poll() == null;

        assertTrue(actual);
    }

    @Test
    @DisplayName("poll() test with non-empty buffer.")
    void givenNonEmptyBufferWhenPollThenReturnElement() {
        String expected = "1";
        buffer = new ConcurrentCircularBuffer<>(new String[]{expected});

        String actual = buffer.poll();

        assertNotNull(actual);
        assertEquals(actual, expected);
    }
}
