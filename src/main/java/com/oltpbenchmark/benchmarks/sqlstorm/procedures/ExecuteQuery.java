package com.oltpbenchmark.benchmarks.sqlstorm.procedures;

import com.oltpbenchmark.api.Procedure;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ExecuteQuery extends Procedure {

  public void run(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      boolean hasResultSet = stmt.execute(sql);
      // Simulate the application fetching the data
      if (hasResultSet) {
        try (ResultSet rs = stmt.getResultSet()) {
          while (rs.next()) {
            // Iterate through results
          }
        }
      }
    }
  }
}
