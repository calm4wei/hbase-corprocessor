package com.cobub.server;

import com.cobub.protobuf.LineCounterServer;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LineCounterEndPoint extends LineCounterServer.LineCounter implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment)
            this.env = (RegionCoprocessorEnvironment) coprocessorEnvironment;
        else throw new CoprocessorException("Must be loaded on a table region!!");
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void countLine(RpcController controller, LineCounterServer.CountRequest request, RpcCallback<LineCounterServer.CountResponse> done) {
        RegionScanner scanner = null;
        LineCounterServer.CountResponse.Builder respBuilder = LineCounterServer.CountResponse.newBuilder();
        if (!"count".equals(request.getAskWord())) {
            respBuilder.setRetWord(23333);
        } else {
            long count = 0;
            try {
                Scan scan = new Scan();
                scan.setMaxVersions(1);
                scanner = env.getRegion().getScanner(scan);
                List<Cell> list = new ArrayList<Cell>();
                while (scanner.next(list))
                    count += 1;
                respBuilder.setRetWord(count);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (scanner != null)
                    try {
                        scanner.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
        }
        done.run(respBuilder.build());
    }
}