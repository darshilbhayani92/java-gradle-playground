package com.darshil.demo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BoundedBlockingQueueTest {

    @Test
    void testPutAndTake() throws InterruptedException {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(3);
        q.put(1);
        q.put(2);
        assertEquals(2, q.size());
        assertEquals(1, q.take());
        assertEquals(2, q.take());
        assertEquals(0, q.size());
    }

    @Test
    void testCapacityBlocking() throws InterruptedException {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(2);
        q.put(10);
        q.put(20);

        Thread producer = new Thread(() -> {
            try {
                q.put(30); // should block until consumer takes
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        producer.start();
        Thread.sleep(100); // give producer time to block
        assertEquals(10, q.take()); // free up space
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertEquals(2, q.size());
    }

    @Test
    void testProducerConsumer() throws InterruptedException {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(3);
        int[] consumed = new int[6];

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 6; i++) {
                try { q.put(i); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });

        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 6; i++) {
                try { consumed[i] = q.take(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });

        producer.start();
        consumer.start();
        producer.join();
        consumer.join();

        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5}, consumed);
    }
}
