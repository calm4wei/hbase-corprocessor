package com.cobub.server;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RegionObserverExample extends BaseRegionObserver {

    private static final byte[] ADMIN = Bytes.toBytes("admin001");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("details");
    private static final byte[] COLUMN = Bytes.toBytes("Admin_det");
    private static final byte[] VALUE = Bytes.toBytes("You can't see Admin details");

    @Override
    public void preGetOp(final ObserverContext e, final Get get, final List results) throws IOException {

        if (Bytes.equals(get.getRow(), ADMIN)) {
            Cell c = CellUtil
                    .createCell(get.getRow(), COLUMN_FAMILY, COLUMN, System.currentTimeMillis(), (byte) 4, VALUE);
            results.add(c);
            e.bypass();
        }

        List kvs = new ArrayList(results.size());
        for (Object c : results) {
            kvs.add(KeyValueUtil.ensureKeyValue((Cell) c));
        }
        //preGet(e, get, kvs);
        results.clear();
        results.addAll(kvs);
    }

    @Override
    public RegionScanner preScannerOpen(final ObserverContext e, final Scan scan, final RegionScanner s)
            throws IOException {

        Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(ADMIN));
        scan.setFilter(filter);
        return s;
    }

    @Override
    public boolean postScannerNext(final ObserverContext e, final InternalScanner s,
            final List results, final int limit, final boolean hasMore) throws IOException {
        Result result = null;
        Iterator iterator = results.iterator();
        while (iterator.hasNext()) {
            result = (Result) iterator.next();
            if (Bytes.equals(result.getRow(), ADMIN)) {
                iterator.remove();
                break;
            }
        }
        return hasMore;
    }

}