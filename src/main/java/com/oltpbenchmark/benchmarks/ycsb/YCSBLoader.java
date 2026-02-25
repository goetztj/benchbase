/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;
import static com.oltpbenchmark.util.SQLUtil.TYPE_INTEGER;
import static com.oltpbenchmark.util.SQLUtil.TYPE_VARCHAR;
import com.oltpbenchmark.util.TextGenerator;

class YCSBLoader extends Loader<YCSBBenchmark> {
  private final int num_record;

  public SQLStmt attachStmt =
      new SQLStmt(
          """
        CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER config, KEY_ID 'admin', SECRET 'password', REGION 'us-east-1', ENDPOINT 'localhost:9000', USE_SSL false, URL_STYLE path);
        ATTACH DATABASE 'ducklake:%s'AS %s (DATA_PATH 's3://warehouse/duckdb/', OVERRIDE_DATA_PATH true);
        """
              .formatted(YCSBConstants.DUCKLAKE_PATH, YCSBConstants.DUCKLAKE_DB));

  public SQLStmt useStmt =
      new SQLStmt(
          """
        USE %s;
        """
              .formatted(YCSBConstants.DUCKLAKE_DB));

  public YCSBLoader(YCSBBenchmark benchmark) {
    super(benchmark);
    this.num_record = (int) Math.round(YCSBConstants.RECORD_COUNT * this.scaleFactor);
    if (LOG.isDebugEnabled()) {
      LOG.debug("# of RECORDS:  {}", this.num_record);
    }
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    List<LoaderThread> threads = new ArrayList<>();
    int count = 0;
    while (count < this.num_record) {
      final int start = count;
      final int stop = Math.min(start + YCSBConstants.THREAD_BATCH_SIZE, this.num_record);
      threads.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("YCSBLoadThread[%d, %d]", start, stop));
              }
              loadRecords(conn, start, stop);
            }
          });
      count = stop;
    }
    return (threads);
  }

  private void loadRecords(Connection conn, int start, int stop) throws SQLException {
    if (getDatabaseType() == DatabaseType.DUCKDB) {
      try (PreparedStatement attach = conn.prepareStatement(attachStmt.getSQL())) {
        attach.execute();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      try (PreparedStatement use = conn.prepareStatement(useStmt.getSQL())) {
        use.execute();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    DatabaseMetaData md = conn.getMetaData();

    String separator = md.getIdentifierQuoteString();

    Table catalog_tbl = new Table("USERTABLE", separator);

    if (this.getDatabaseType() == DatabaseType.DUCKDB) {
      catalog_tbl.addColumn(
          new Column("ycsb_key", separator, catalog_tbl, TYPE_INTEGER, null, false));
      catalog_tbl.addColumn(new Column("field1", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field2", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field3", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field4", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field5", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field6", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field7", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field8", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(new Column("field9", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
      catalog_tbl.addColumn(
          new Column("field10", separator, catalog_tbl, TYPE_VARCHAR, 100, false));
    } else {
      catalog_tbl = benchmark.getCatalog().getTable("USERTABLE");
    }

    String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      long total = 0;
      int batch = 0;
      for (int i = start; i < stop; i++) {
        stmt.setInt(1, i);
        for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
          stmt.setString(j + 2, TextGenerator.randomStr(rng(), benchmark.fieldSize));
        }
        stmt.addBatch();
        total++;
        if (++batch >= workConf.getBatchSize()) {
          stmt.executeBatch();

          batch = 0;
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
          }
        }
      }
      if (batch > 0) {
        stmt.executeBatch();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished loading {}", catalog_tbl.getName());
    }
  }
}
