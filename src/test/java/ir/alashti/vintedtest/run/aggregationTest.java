package ir.alashti.vintedtest.run;

import ir.alashti.vintedtest.dataset.*;
import ir.alashti.vintedtest.dataset.input.LoaderManager;
import ir.alashti.vintedtest.dataset.output.WriterManager;
import ir.alashti.vintedtest.dataset.parallel.ParallelDataset;
import ir.alashti.vintedtest.dataset.parallel.ParallelPairDataset;
import ir.alashti.vintedtest.dataset.udf.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class aggregationTest {

    @BeforeAll
    public static void setUp() {
    }

    @Test
    public void aggregate() {
        List<String> clicksHeader = new ArrayList<>();
        clicksHeader.add("date");
        clicksHeader.add("screen");
        clicksHeader.add("user_id");
        clicksHeader.add("click_target");
        ParallelDataset<Row> clicks = LoaderManager.textLoaderManager("data/clicks", clicksHeader, true, Formats.CSV.getSplitter());
        ParallelPairDataset<String, Integer> result = clicks.mapToPair(d -> new Tuple2<>((String) ((Row) d).getValues().get(0), 1))
                .reduce((s, i1, i2) -> i1 + i2);
        WriterManager.writerManager(result.collect(), "aggregation.csv");
        Assertions.assertEquals(result.count(), 12);
    }

}