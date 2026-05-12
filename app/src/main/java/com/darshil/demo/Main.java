package com.darshil.demo;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== App ===");
        System.out.println(new App().getGreeting());

        System.out.println("\n=== BoundedBlockingQueue ===");
        runBoundedBlockingQueue();

        System.out.println("\n=== NewBlockingQueue ===");
        runNewBlockingQueue();
    }

    private static void runBoundedBlockingQueue() throws InterruptedException {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(3);

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 6; i++) {
                try {
                    q.put(i);
                    System.out.println("Produced: " + i);
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });

        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 6; i++) {
                try {
                    Thread.sleep(200);
                    System.out.println("Consumed: " + q.take());
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });

        producer.start(); consumer.start();
        producer.join();  consumer.join();
    }

    private static void runNewBlockingQueue() throws InterruptedException {
        NewBlockingQueue<Integer> q = new NewBlockingQueue<>(3);

        Thread producer = new Thread(new NewBlockingQueue.Producer(q, 6));
        Thread consumer = new Thread(new NewBlockingQueue.Consumer(q, 6));

        producer.start(); consumer.start();
        producer.join();  consumer.join();

        System.out.println("Done. Queue size: " + q.size());
    }
}
