package com.opentext.test;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TaskExecutorServiceTest {

    private int maxConcurrentTasks = 5;

    @Test
    public void testTaskOrder() throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutorService = new TaskExecutorService(maxConcurrentTasks);
        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup3 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<Integer> task1 = new Main.Task<>(UUID.randomUUID(), taskGroup1, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 1);
            return 1;
        });
        Main.Task<Integer> task11 = new Main.Task<>(UUID.randomUUID(), taskGroup1, Main.TaskType.READ, () -> {
            Thread.sleep(1000);
            System.out.println(Thread.currentThread().getName() + " Running "+ 11);
            return 11;
        });
        Main.Task<Integer> task111 = new Main.Task<>(UUID.randomUUID(), taskGroup1, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 111);
            return 111;
        });
        Main.Task<Integer> task2 = new Main.Task<>(UUID.randomUUID(), taskGroup2, Main.TaskType.READ, () -> {
            Thread.sleep(2000);
            System.out.println(Thread.currentThread().getName() + " Running "+ 2);
            return 2;
        });
        Main.Task<Integer> task22 = new Main.Task<>(UUID.randomUUID(), taskGroup2, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 22);
            return 22;
        });
        Main.Task<Integer> task222 = new Main.Task<>(UUID.randomUUID(), taskGroup2, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 222);
            return 222;
        });
        Main.Task<Integer> task3 = new Main.Task<>(UUID.randomUUID(), taskGroup3, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 3);
            return 3;
        });
        Main.Task<Integer> task33 = new Main.Task<>(UUID.randomUUID(), taskGroup3, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 33);
            return 33;
        });
        Main.Task<Integer> task333 = new Main.Task<>(UUID.randomUUID(), taskGroup3, Main.TaskType.READ, () -> {
            System.out.println(Thread.currentThread().getName() + " Running "+ 333);
            return 333;
        });

        Future<Integer> futureRes1 = taskExecutorService.submitTask(task1);
        taskExecutorService.submitTask(task11);
        taskExecutorService.submitTask(task111);
        Future<Integer> futureRes2 = taskExecutorService.submitTask(task2);
        taskExecutorService.submitTask(task22);
        taskExecutorService.submitTask(task222);
        Future<Integer> futureRes3 = taskExecutorService.submitTask(task3);
        taskExecutorService.submitTask(task33);
        taskExecutorService.submitTask(task333);

        Assert.assertFalse(1 != futureRes1.get());
        Assert.assertFalse(2 != futureRes2.get());
        Assert.assertFalse(3 != futureRes3.get());
        taskExecutorService.shutdown();
    }

    @Test
    public void testGroupSynchronization() throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutorService = new TaskExecutorService(maxConcurrentTasks);
        Main.TaskGroup taskGroup = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<Integer> task1 = new Main.Task<>(UUID.randomUUID(), taskGroup, Main.TaskType.READ, () -> {
            Thread.sleep(1000);
            return 1;
        });
        Main.Task<Integer> task2 = new Main.Task<>(UUID.randomUUID(), taskGroup, Main.TaskType.READ, () -> 2);

        Future<Integer> futureRes1 = taskExecutorService.submitTask(task1);
        Future<Integer> futureRes2 = taskExecutorService.submitTask(task2);

        Assert.assertFalse(futureRes1.isDone());

        Assert.assertFalse(futureRes2.isDone());

        Assert.assertFalse(1 != futureRes1.get());
        Assert.assertFalse(2 != futureRes2.get());

        taskExecutorService.shutdown();
    }

    @Test
    public void testConcurrencyBetweenGroups() throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutorService = new TaskExecutorService(maxConcurrentTasks);

        Main.TaskGroup taskGroup = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<Integer> task1 = new Main.Task<>(UUID.randomUUID(), taskGroup, Main.TaskType.READ, () -> {
            Thread.sleep(1000);
            return 1;
        });
        Main.Task<Integer> task2 = new Main.Task<>(UUID.randomUUID(), taskGroup2, Main.TaskType.READ, () -> 2);

        Future<Integer> futureRes1 = taskExecutorService.submitTask(task1);
        Future<Integer> futureRes2 = taskExecutorService.submitTask(task2);

        Assert.assertEquals(2l, (long) futureRes2.get());
        Assert.assertEquals(1l, (long) futureRes1.get());

        taskExecutorService.shutdown();

    }

    @Test
    public void testNullTask() {
        TaskExecutorService taskExecutorService = new TaskExecutorService(maxConcurrentTasks);
        Future<Integer> futureRes1 = taskExecutorService.submitTask(null);
        try {
            futureRes1.get();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof NullPointerException);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        taskExecutorService.shutdown();
    }

    @Test
    public void testShutdown() {
        TaskExecutorService taskExecutorService = new TaskExecutorService(maxConcurrentTasks);
        Main.TaskGroup taskGroup = new Main.TaskGroup(UUID.randomUUID());
        Main.Task<Integer> task1 = new Main.Task<>(UUID.randomUUID(), taskGroup, Main.TaskType.READ, () -> {
            Thread.sleep(2000);
            return 1;
        });

        Future<Integer> futureRes = taskExecutorService.submitTask(task1);
        taskExecutorService.shutdown();

        Assert.assertTrue(futureRes.isDone());

    }

    @Test
    public void testHandlingLargeNoOfTasks() throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutorService = new TaskExecutorService(maxConcurrentTasks);
        Main.TaskGroup taskGroup = new Main.TaskGroup(UUID.randomUUID());
        List<Future<Integer>> futureList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            Main.Task<Integer> task = new Main.Task<>(UUID.randomUUID(), taskGroup, Main.TaskType.READ, () -> finalI);
            futureList.add(taskExecutorService.submitTask(task));
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, (long) futureList.get(i).get());
        }
    }
}
