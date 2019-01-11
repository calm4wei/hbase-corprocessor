package com.cobub.client;

import com.cobub.protobuf.LineCounterServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class LineCounterClient {

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master001:2181,slave001:2181,slave003:2181");
        // conf.set("hbase.master", "host_master:60000");
        HTable hTable = new HTable(conf, "test:test");
//        final LineCounterServer.CountRequest req = LineCounterServer.CountRequest.newBuilder().setAskWord("count").build();
//        Map<byte[], Long> tmpRet = table.coprocessorService(LineCounterServer.LineCounter.class, null, null, new Batch.Call<LineCounterServer.LineCounter, Long>() {
//            @Override
//            public Long call(LineCounterServer.LineCounter instance) throws IOException {
//                ServerRpcController controller = new ServerRpcController();
//                BlockingRpcCallback<LineCounterServer.CountResponse> rpc = new BlockingRpcCallback<LineCounterServer.CountResponse>();
//                instance.countLine(controller, req, rpc);
//                LineCounterServer.CountResponse resp = rpc.get();
//                return resp.getRetWord();
//            }
//        });
//        long ret = 0;
//        for (long l : tmpRet.values())
//            ret += l;
//        System.out.println("lines: " + ret);


        Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("tiem"), Bytes.toBytes("time"));
        hTable.put(put);

        hTable.close();
    }
}