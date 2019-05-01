import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * Measure calcite overhead by (also) directly querying the source (in our case HSQLDB).
 */
public class CalciteOverheadTest {

  /**
   * Number of columns in the table (not all of them are queried)
   */
  private static final int COLUMN_COUNT = 1000;

  /**
   * Number of rows to populate
   */
  private static final int ROW_COUNT = 1000;

  // connection which is using calcite library
  private Connection calciteConnection;

  // direct connection to in-memory database HSQLDB. This is used as baseline
  private DataSource dataSource;

  // used to measure latency percentiles
  private final Tracer tracer = new Tracer();

  @Before
  public void setUp() throws Exception {
    this.dataSource = createDataSource();
    createDummyTable(dataSource);

    // create calcite schemas and connection
    Class.forName("org.apache.calcite.jdbc.Driver");
    this.calciteConnection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    final SchemaPlus rootSchema = calciteConnection.unwrap(CalciteConnection.class).getRootSchema();
    final Schema schema = JdbcSchema.create(rootSchema, "mytest", dataSource,  null, null);
    rootSchema.add("mytest",  schema);
    calciteConnection.setSchema("mytest"); // default schema
  }

  private static DataSource createDataSource() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
    dataSource.setUrl("jdbc:hsqldb:mem:mytest");
    dataSource.setUsername("sa");
    dataSource.setPassword("");
    return dataSource;
  }

  /**
   * Create table like:
   *
   * <pre>
   *  {@code
   *    CREATE TABLE dummy (
   *       id PRIMARY KEY,
   *       col1 INTEGER,
   *       ....
   *       colN INTEGER
   *   );
   *  }
   * </pre>
   */
  private static void createDummyTable(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.createStatement().execute("DROP TABLE IF EXISTS dummy;");
      // column names: col1, col2, ...
      final List<String> names =  Stream.iterate(0, i -> i + 1)
          .map(i -> String.format("COL%d", i))
          .limit(COLUMN_COUNT)
          .collect(Collectors.toList());

      try (Statement stm = connection.createStatement()) {
        stm.execute("create table dummy (id INTEGER IDENTITY PRIMARY KEY, "
            + names.stream().map(c -> c + " INTEGER").collect(Collectors.joining(", "))
            + ");");
      }

      final String insert = "INSERT INTO dummy (" + String.join(",", names) + ") VALUES ("
          + String.join(",", Collections.nCopies(COLUMN_COUNT, "?")) + ")";

      try (PreparedStatement stm = connection.prepareStatement(insert)) {
        for (int i = 0; i < COLUMN_COUNT; i++) {
          stm.setInt(i + 1, i);
        }

        for (int i = 0; i < ROW_COUNT; i++) {
          stm.execute();
        }
      }

    }
  }

  @After
  public void tearDown() throws Exception {
    if (calciteConnection != null) {
      calciteConnection.close();
    }

    if (dataSource instanceof AutoCloseable) {
      ((AutoCloseable) dataSource).close();
    }

    System.out.println(tracer.toString());

  }

  @Test
  public void cols10() {
    execute(10);
  }

  @Test
  public void cols100() {
    execute(100);
  }

  @Test
  public void cols500() {
    execute(500);
  }

  @Test
  public void cols1000() {
    execute(1000);
  }

  /**
   * Generates and executes a query against both calcite and HSQLDB.
   * <p>The query is typically of type:
   * {@code SELECT min(col1), max(col2), avg(col3), ... from dummy}
   * @param columnCount number of columns in {@code select} clause.
   */
  private void execute(int columnCount)  {
    System.out.println("=== Column columnCount: " + columnCount);
    final List<String> aggs = Arrays.asList("MIN", "MAX", "SUM", "COUNT");
    final String select = Stream.iterate(0, i -> i + 1)
        .map(i -> String.format("%s(COL%s)", aggs.get(i % aggs.size()), i))
        .limit(columnCount)
        .collect(Collectors.joining(", "));

    final String sql = "select " + select + " from DUMMY where ID <> 0";

    try {
      warmup(sql);
    } catch (InterruptedException e) {
      return;
    }

    for (int i = 0; i < 200; i++) {
      tracer.trace("calcite", () -> queryCalcite(sql));
      tracer.trace("direct", () -> queryDirect(sql));
    }
  }

  /**
   * Does a couple of passes to populate caches and enable JVM optimizations
   */
  private void warmup(String sql) throws InterruptedException {
    // warmup
    for (int i = 0; i < 100; i++) {
      queryCalcite(sql);
      queryDirect(sql);
      if (i % 10 == 0) {
        Thread.sleep(100);
      }
    }

    Thread.sleep(1000);
    System.gc(); // probably not needed
  }

  /**
   * Traces latencies for a Runnable.
   */
  private static class Tracer  {

    private final Multimap<String, Long> latencies = ArrayListMultimap.create();
    private final TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    public void trace(String label, Runnable runnable) {
      final long start = System.nanoTime();
      try {
        runnable.run();
      } finally {
        final long end = System.nanoTime();
        final long units = timeUnit.convert(end - start, TimeUnit.NANOSECONDS);
        latencies.get(label).add(units);
      }
    }

    @Override public String toString() {
      final double[] percentiles = {.5, .8, .9, .95};

      final StringBuilder builder = new StringBuilder();
      for (String label: latencies.keySet()) {
        final List<Long> latencies = new ArrayList<>(this.latencies.get(label));
        if (!latencies.isEmpty()) {
          Collections.sort(latencies);
          List<String> out = new ArrayList<>();
          out.add(label);
          out.add("count:" + latencies.size());
          for (double pct: percentiles) {
            long latency = latencies.get((int) Math.floor(latencies.size() * pct));
            out.add(String.format("%d%%:%d", (int) (pct * 100), latency));
          }
          out.add("max:" + latencies.get(latencies.size() - 1));
          builder.append(String.join(" ", out));
        } else {
          builder.append(label).append(" was empty");
        }
        builder.append(System.lineSeparator());
      }

      return builder.toString();
    }
  }

  private void queryCalcite(String sql) {
    query(calciteConnection, sql);
  }

  private void queryDirect(String sql)  {
    try (Connection connection = dataSource.getConnection()) {
      query(connection, sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void query(Connection connection, String sql) {
    try (PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rset = statement.executeQuery()) {
      consume(rset);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Ensure results of the query are consumed to avoid any lazy caching.
   */
  private static void consume(ResultSet resultSet) throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    assertTrue(meta.getColumnCount() > 0);

    int rows = 0;
    long sum = 0;
    while (resultSet.next()) {
      rows++;
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        final int columnType = meta.getColumnType(i);
        if (columnType == Types.NUMERIC ||
            columnType == Types.TINYINT ||
           columnType == Types.BIGINT) {
          sum += resultSet.getInt(i);
        }
      }
    }

    if (rows > 0) {
       assertTrue("result set sum should be positive", sum > 0);
    }
  }
}
