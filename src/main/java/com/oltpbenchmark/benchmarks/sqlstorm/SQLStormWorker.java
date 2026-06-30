package com.oltpbenchmark.benchmarks.sqlstorm;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.sqlstorm.procedures.ExecuteQuery;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;

public class SQLStormWorker extends Worker<SQLStormBenchmark> {

  public SQLStormWorker(SQLStormBenchmark benchmarkModule, int id) {
    super(benchmarkModule, id);
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType txnType)
      throws UserAbortException, SQLException {
    SQLStormBenchmark parentBenchmark = this.getBenchmark();
    String sql = parentBenchmark.getRandomQuery();

    if (sql == null) {
      return TransactionStatus.RETRY;
    }

    ExecuteQuery proc = this.getProcedure(ExecuteQuery.class);
    proc.run(conn, sql);

    return TransactionStatus.SUCCESS;
  }
}
