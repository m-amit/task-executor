package com.opentext.test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutorService implements Main.TaskExecutor {

    private final BlockingQueue<Runnable> taskQueue;
    private final ExecutorService executor;
    private final Map<UUID, ReentrantLock> taskLocks;

    public TaskExecutorService(int maxConcurrentTasks) {
        this.taskQueue = new LinkedBlockingQueue<>();
        this.executor = Executors.newFixedThreadPool(maxConcurrentTasks);
        this.taskLocks = new ConcurrentHashMap<>();

        for (int i = 0; i < maxConcurrentTasks; i++) {
            executor.submit(this::processTasks);
        }
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        if (task == null) {
            future.completeExceptionally(new NullPointerException("task is null"));
            return future;
        }
        Runnable runnable = () -> {
            ReentrantLock lock = taskLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new ReentrantLock());
            lock.lock();
            try {
                T futureTask = task.taskAction().call();
                future.complete(futureTask);
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                lock.unlock();
            }
        };
        taskQueue.add(runnable);
        return future;
    }

    private void processTasks() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Runnable task = taskQueue.take();
                task.run();
            } catch (InterruptedException e) {
                System.out.println("waiting on queue for new task interrupted");
                break;
            }
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
