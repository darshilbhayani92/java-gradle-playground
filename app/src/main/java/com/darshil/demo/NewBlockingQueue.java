package com.darshil.demo;

import java.util.concurrent.locks.*;
import java.util.*;

public class NewBlockingQueue<T> {

    private final Queue<T> queue;
    private final int capacity;
    private final ReentrantLock lock;
    private final Condition notFull;
    private final Condition notEmpty;

    public NewBlockingQueue(int capacity) {
        this.capacity = capacity;
        this.queue    = new LinkedList<>();
        this.lock     = new ReentrantLock();
        this.notFull  = lock.newCondition();
        this.notEmpty = lock.newCondition();
    }

    public void put(T item) throws InterruptedException {
        lock.lock();
        try {
            while (queue.size() == capacity) notFull.await();
            queue.add(item);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public T take() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) notEmpty.await();
            T item = queue.poll();
            notFull.signal();
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

    public static class Producer implements Runnable {
        private final NewBlockingQueue<Integer> queue;
        private final int count;

        public Producer(NewBlockingQueue<Integer> queue, int count) {
            this.queue = queue;
            this.count = count;
        }

        @Override
        public void run() {
            for (int i = 0; i < count; i++) {
                try {
                    queue.put(i);
                    System.out.println("[Producer] put: " + i);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static class Consumer implements Runnable {
        private final NewBlockingQueue<Integer> queue;
        private final int count;

        public Consumer(NewBlockingQueue<Integer> queue, int count) {
            this.queue = queue;
            this.count = count;
        }

        @Override
        public void run() {
            for (int i = 0; i < count; i++) {
                try {
                    Thread.sleep(150);
                    int val = queue.take();
                    System.out.println("[Consumer] took: " + val);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
