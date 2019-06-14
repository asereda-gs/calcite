/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to you under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.calcite.test;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

import com.google.common.util.concurrent.MoreExecutors;

import javax.sql.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Query calcite connection in parallel with a simple SQL {@code select * from table}.
 *
 * <p>Getting the following exception
 * <pre>
 *  {@code
 *  Caused by: org.apache.calcite.avatica.NoSuchStatementException
 *    at org.apache.calcite.jdbc.CalciteConnectionImpl$CalciteServerImpl.getStatement(CalciteConnectionImpl.java:371)
 *    at org.apache.calcite.jdbc.CalciteConnectionImpl.getCancelFlag(CalciteConnectionImpl.java:238)
 * 	  at org.apache.calcite.avatica.AvaticaStatement.<init>(AvaticaStatement.java:121)
 *  }
 * </pre>
 *
 * <p>Issue seems to be in {@code org.apache.calcite.avatica.MetaImpl.java:213}
 *
 *  <pre>
 *  {@code
 *    // not atomic increment of id
 *    return new StatementHandle(ch.id, connection.statementCount++, null);
 *  }
 *  </pre>
 */
public class CalciteConcurrentTest {
  private final ExecutorService executor = Executors.newFixedThreadPool(10);

  /**
   * Connection pool to underlying database (hsqldb)
   */
  private DataSource dataSource;

  /**
   * Calcite connection ("singleton")
   */
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    this.dataSource = createDataSource();

    try (Connection connection = dataSource.getConnection()) {
      connection.createStatement().execute("DROP TABLE IF EXISTS dummy;");
      try (Statement stm = connection.createStatement()) {
        stm.execute("create table dummy (id INTEGER IDENTITY PRIMARY KEY);");
        for (int i = 0; i < 10; i++) {
          stm.execute(String.format("INSERT INTO dummy (id) VALUES (%d);", i));
        }
      }
    }

    Class.forName("org.apache.calcite.jdbc.Driver");
    final Connection connection = DriverManager.getConnection("jdbc:calcite:");
    final SchemaPlus rootSchema = connection.unwrap(CalciteConnection.class).getRootSchema();
    final JdbcSchema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, null);
    rootSchema.add("test", schema);

    this.connection = connection; // calcite connection
  }


  /**
   * Close all connections
   */
  @After
  public void tearDown() throws Exception {
    // shutdown executor
    MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);

    if (connection != null) {
      connection.close();
    }

    if (dataSource instanceof AutoCloseable) {
      ((AutoCloseable) dataSource).close();
    }
  }

  private static DataSource createDataSource() throws SQLException {
    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName("org.hsqldb.jdbcDriver");
    ds.setUrl("jdbc:hsqldb:mem:db:leak");
    ds.setMaxTotal(15);
    return ds;
  }

  @Test public void test() throws Exception {
    final Connection connection = this.connection;
    final List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      futures.add(executor.submit(() -> execute(connection)));
    }

    final List<Exception> errors = new ArrayList<>();
    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        errors.add(e);
      }
    }

    if (!errors.isEmpty()) {
      errors.forEach(Throwable::printStackTrace);
      Assert.fail(String.format("Failed with %d exceptions (see stack trace for details)", errors.size()));
    }
  }


  /**
   * Executes simple query {@code select * from table} and checks that result is non-empty.
   */
  private static void execute(Connection connection) {
    try (Statement stm = connection.createStatement();
        ResultSet resultSet = stm.executeQuery("select * from \"test\".\"DUMMY\"")) {
      int rows = 0;
      while (resultSet.next()) {
        rows++;
      }
      assertTrue("no rows", rows > 0);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

// End CalciteConcurrentTest.java
