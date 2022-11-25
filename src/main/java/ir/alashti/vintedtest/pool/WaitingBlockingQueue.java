package ir.alashti.vintedtest.pool;

import java.util.concurrent.ArrayBlockingQueue;

public class WaitingBlockingQueue extends ArrayBlockingQueue {
    public WaitingBlockingQueue(int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(Object o) {
        try {
            this.put(o);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
}
