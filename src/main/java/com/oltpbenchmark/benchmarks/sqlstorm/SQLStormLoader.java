package com.oltpbenchmark.benchmarks.sqlstorm;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import java.sql.SQLException;
import java.util.List;

public class SQLStormLoader extends Loader<SQLStormBenchmark> {

  public SQLStormLoader(SQLStormBenchmark benchmark) {
    super(benchmark);
  }

  @Override
  public List<LoaderThread> createLoaderThreads() throws SQLException {
    // Empty list since data is read from MinIO
    return List.of();
  }
}
