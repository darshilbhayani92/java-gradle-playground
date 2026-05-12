package com.darshil.demo;

import java.util.concurrent.locks.*;
import java.util.*;

public class BoundedBlockingQueue<T> {

    private final Queue<T> queue;
    private final int capacity;
    private final ReentrantLock lock;
    private final Condition notFull;   // signal when space available
    private final Condition notEmpty;  // signal when item available

    public BoundedBlockingQueue(int capacity) {
        this.capacity = capacity;
        this.queue    = new LinkedList<>();
        this.lock     = new ReentrantLock();
        this.notFull  = lock.newCondition();
        this.notEmpty = lock.newCondition();
    }

    // Producer: blocks if full
    public void put(T item) throws InterruptedException {
        lock.lock();
        try {
            // MUST be while, not if — spurious wakeups!
            while (queue.size() == capacity) {
                notFull.await();
            }
            queue.add(item);
            notEmpty.signal(); // wake one consumer
        } finally {
            lock.unlock(); // ALWAYS in finally!
        }
    }

    // Consumer: blocks if empty
    public T take() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                notEmpty.await();
            }
            T item = queue.poll();
            notFull.signal(); // wake one producer
            return item;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try { return queue.size(); }
        finally { lock.unlock(); }
    }

    // Test
    public static void main(String[] args) throws Exception {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(3);

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 6; i++) {
                try {
                    q.put(i);
                    System.out.println("Produced: " + i);
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });

        Thread conser = new Thread(() -> {
            for (int i = 0; i < 6; i++) {
                try {
                    Thread.sleep(200);
                    System.out.println("Consumed: " + q.take());
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });

        producer.start(); conser.start();
        producer.join();  conser.join();
    }
}
