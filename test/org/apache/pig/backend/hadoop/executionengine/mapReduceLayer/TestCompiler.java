package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.pigunit.pig.PigServer;
import org.junit.Test;

public class TestCompiler {

    @Test
    public void testCompiler() throws IOException {
        final PigServer pigServer = new PigServer(ExecType.LOCAL);
        final Data data = Storage.resetData(pigServer);
        data.set("in", "a:int", tuple(1), tuple(2));
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
        pigServer.registerQuery("B = FOREACH A GENERATE $0 * $0 + $0;");
        pigServer.registerQuery("C = FOREACH A GENERATE $0 - $0;");
        pigServer.registerQuery("D = FILTER A BY $0 == 1 ;");
        pigServer.registerQuery("STORE B INTO 'out' USING mock.Storage();");
        pigServer.registerQuery("STORE C INTO 'out2' USING mock.Storage();");
        pigServer.registerQuery("STORE D INTO 'out3' USING mock.Storage();");
        List<ExecJob> jobs = pigServer.executeBatch();
        for (ExecJob job : jobs) {
          assertEquals(JOB_STATUS.COMPLETED, job.getStatus());
        }
        assertEquals(Arrays.asList(tuple(2), tuple(6)), data.get("out"));
        assertEquals(Arrays.asList(tuple(0), tuple(0)), data.get("out2"));
        assertEquals(Arrays.asList(tuple(1)), data.get("out3"));
        System.out.println(data.get("out"));
    }

}
