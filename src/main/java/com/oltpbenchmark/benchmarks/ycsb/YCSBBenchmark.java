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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;
import static com.oltpbenchmark.util.SQLUtil.TYPE_INTEGER;
import static com.oltpbenchmark.util.SQLUtil.TYPE_VARCHAR;

public final class YCSBBenchmark extends BenchmarkModule {

  private static final Logger LOG = LoggerFactory.getLogger(YCSBBenchmark.class);

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

  /** The length in characters of each field */
  protected final int fieldSize;

  /** The constant used in the zipfian distribution (to modify the skew) */
  protected final double skewFactor;

  public YCSBBenchmark(WorkloadConfiguration workConf) {
    super(workConf);

    int fieldSize = YCSBConstants.MAX_FIELD_SIZE;
    if (workConf.getXmlConfig() != null && workConf.getXmlConfig().containsKey("fieldSize")) {
      fieldSize =
          Math.min(workConf.getXmlConfig().getInt("fieldSize"), YCSBConstants.MAX_FIELD_SIZE);
    }
    this.fieldSize = fieldSize;
    if (this.fieldSize <= 0) {
      throw new RuntimeException("Invalid YCSB fieldSize '" + this.fieldSize + "'");
    }

    double skewFactor = 0.99;
    if (workConf.getXmlConfig() != null && workConf.getXmlConfig().containsKey("skewFactor")) {
      skewFactor = workConf.getXmlConfig().getDouble("skewFactor");
      if (skewFactor <= 0 || skewFactor >= 1) {
        throw new RuntimeException("Invalid YCSB skewFactor '" + skewFactor + "'");
      }
    }
    this.skewFactor = skewFactor;
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    try {
      // LOADING FROM THE DATABASE IMPORTANT INFORMATION
      // LIST OF USERS
      DatabaseMetaData md = this.makeConnection().getMetaData();

      String separator = md.getIdentifierQuoteString();

      Table t = new Table("USERTABLE", separator);

      if (this.getWorkloadConfiguration().getDatabaseType() == DatabaseType.DUCKDB) {
        t.addColumn(new Column("ycsb_key", separator, t, TYPE_INTEGER, null, false));
        t.addColumn(new Column("field1", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field2", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field3", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field4", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field5", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field6", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field7", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field8", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field9", separator, t, TYPE_VARCHAR, 100, false));
        t.addColumn(new Column("field10", separator, t, TYPE_VARCHAR, 100, false));
      } else {
        t = this.getCatalog().getTable("USERTABLE");
      }

      String userCount = SQLUtil.getMaxColSQL(this.workConf.getDatabaseType(), t, "ycsb_key");

      if (this.getWorkloadConfiguration().getDatabaseType() == DatabaseType.DUCKDB) {
        Connection metaConn = this.makeConnection();
        try (PreparedStatement attach = metaConn.prepareStatement(attachStmt.getSQL())) {
          attach.execute();
        } catch (SQLException ignore) {
        }

        try (PreparedStatement use = metaConn.prepareStatement(useStmt.getSQL())) {
          use.execute();
        }

        try (Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(userCount)) {
          int init_record_count = 0;
          while (res.next()) {
            init_record_count = res.getInt(1);
          }

          for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new YCSBWorker(this, i, init_record_count + 1));
          }
        }
      } else {

        try (Connection metaConn = this.makeConnection();
            Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(userCount)) {
          int init_record_count = 0;
          while (res.next()) {
            init_record_count = res.getInt(1);
          }

          for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new YCSBWorker(this, i, init_record_count + 1));
          }
        }
      }
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
    }
    return workers;
  }

  @Override
  protected Loader<YCSBBenchmark> makeLoaderImpl() {
    return new YCSBLoader(this);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return InsertRecord.class.getPackage();
  }
}
