package com.jwszol;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JoinJobTest { ;
    private transient JavaSparkContext sc;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "SparkJoinsTest");
    }

    @After
    public void tearDown() {
        if (sc != null){
            sc.stop();
        }
    }

    @Test
    public void testExampleJob() {
        String path = "./src/main/resources/199607daily.txt";
        TransactionAnalyseJob job = new TransactionAnalyseJob(sc);

        job.read_data(path);
        job.print_data();
        job.processing_data_for_wban_Number();
        job.processing_data_for_specyfic_days();
    }
}
