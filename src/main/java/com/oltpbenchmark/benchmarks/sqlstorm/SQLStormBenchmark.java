package com.oltpbenchmark.benchmarks.sqlstorm;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLStormBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(SQLStormBenchmark.class);
  private final List<String> sharedQueries = new ArrayList<>();

  public SQLStormBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
    loadQueriesOnce();
  }

  private void loadQueriesOnce() {
    String queryDir = workConf.getXmlConfig().getString("queryDir", "/tmp/sqlstorm_queries");
    File dir = new File(queryDir);
    if (dir.exists() && dir.isDirectory()) {
      File[] files = dir.listFiles((d, name) -> name.endsWith(".sql"));
      if (files != null) {
        for (File file : files) {
          try {
            sharedQueries.add(new String(Files.readAllBytes(file.toPath())));
          } catch (Exception e) {
            LOG.error("Failed to read: {}", file.getName());
          }
        }
      }
    }
    LOG.info("Loaded {} queries for an ongoing stream.", sharedQueries.size());
  }

  public String getRandomQuery() {
    if (sharedQueries.isEmpty()) return null;
    int randomIndex = ThreadLocalRandom.current().nextInt(sharedQueries.size());
    return sharedQueries.get(randomIndex);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return com.oltpbenchmark.benchmarks.sqlstorm.procedures.ExecuteQuery.class.getPackage();
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    for (int i = 0; i < workConf.getTerminals(); ++i) {
      workers.add(new SQLStormWorker(this, i));
    }
    return workers;
  }

  @Override
  protected Loader<SQLStormBenchmark> makeLoaderImpl() {
    return new SQLStormLoader(this);
  }
}
