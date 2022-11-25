package ir.alashti.vintedtest.dataset.output;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Writer<T> implements Runnable {
    static ReentrantReadWriteLock.WriteLock writeLock = new ReentrantReadWriteLock().writeLock();

    List<T> rows;
    String outputPath;

    public Writer(List<T> rows, String outputPath) {
        this.rows = rows;
        this.outputPath = outputPath;
    }

    @Override
    public void run() {
        try (BufferedWriter bfw = Files.newBufferedWriter(Paths.get(outputPath), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
            writeLock.lock();
            for (T entry : rows) {
                bfw.write(entry.toString());
                bfw.newLine();
            }
            bfw.flush();
            writeLock.unlock();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
