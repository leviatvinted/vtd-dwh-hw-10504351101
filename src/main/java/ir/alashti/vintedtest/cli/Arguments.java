package ir.alashti.vintedtest.cli;

import org.kohsuke.args4j.Option;

public class Arguments {
    @Option(name = "-c", aliases = "--cores", required = true, usage = "number of executor threads")
    public static int cores = 2;

    public int getCores() {
        return cores;
    }
}
