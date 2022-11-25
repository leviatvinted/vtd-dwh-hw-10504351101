package ir.alashti.vintedtest.cli;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConfigLoader {
    private static volatile Arguments arguments;
    static ReentrantReadWriteLock.WriteLock reentrantWriteLock = new ReentrantReadWriteLock().writeLock();

    private ConfigLoader() {
    }

    public static Arguments initConfigs(String[] args) {
        if (arguments == null) {
            reentrantWriteLock.lock();
            if (arguments == null) {
                arguments = new Arguments();
                CmdLineParser parser = new CmdLineParser(arguments);
                try {
                    parser.parseArgument(args);
                } catch (CmdLineException e) {
                    parser.printUsage(System.out);
                }
            }
            reentrantWriteLock.unlock();
        }
        return arguments;
    }
}
