package com.cobub.server;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class RowCountObserver extends BaseRegionObserver {

    HTableInterface hTable;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        hTable = e.getTable(TableName.valueOf("test", "test"));
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (null != hTable) {
            hTable.close();
        }
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
            throws IOException {
        //super.preGetOp(e, get, results);
        Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString().replaceAll("-", "")));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("uuid"), Bytes.toBytes(1));
        hTable.put(put);

    }


    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("uuid"), Bytes.toBytes(UUID.randomUUID().toString().replaceAll("-", "")));
        hTable.put(put);
    }


    // override prePut(), preDelete(), etc.
}