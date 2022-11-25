package ir.alashti.vintedtest.pool;

import ir.alashti.vintedtest.cli.Arguments;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Executor {
    private static volatile ThreadPoolExecutor executorPool;
    static ReentrantReadWriteLock.WriteLock reentrantWriteLock = new ReentrantReadWriteLock().writeLock();
    private Executor() {
    }

    public static ThreadPoolExecutor getExecutorPool() {
        if (executorPool == null) {
            reentrantWriteLock.lock();
            if (executorPool == null)
                executorPool = new ThreadPoolExecutor(Arguments.cores, Arguments.cores, 0, TimeUnit.MILLISECONDS, new WaitingBlockingQueue(Arguments.cores));
            reentrantWriteLock.unlock();
        }
        return executorPool;
    }
}
