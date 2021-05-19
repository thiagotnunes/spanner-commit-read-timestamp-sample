/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {


  private static final String PROJECT = "appdev-soda-spanner-staging";
  private static final String INSTANCE = "thiagotnunes-test-instance";
  private static final String DATABASE = "example-db";
  private static final String URL = String
      .format("jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
          PROJECT, INSTANCE, DATABASE);
  private static final String RESET = "\033[0m";
  private static final String RED = "\033[0;31m";

  public static void main(String[] args)
      throws SQLException, ExecutionException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final BlockingQueue<Long> queue = new LinkedBlockingQueue<>();
    final long numberOfElements = 100;

    deleteAllFromSingers();

    // Producer thread
    final Future<List<Timestamp>> commitTimestampsFuture = executor
        .submit(producer(queue, numberOfElements));
    // Consumer thread
    final Future<List<Timestamp>> readTimestampsFuture = executor
        .submit(consumer(queue, numberOfElements));

    compareReadAndCommitTimestamps(numberOfElements, commitTimestampsFuture, readTimestampsFuture);

    executor.shutdown();
  }

  private static void compareReadAndCommitTimestamps(
      long numberOfElements,
      Future<List<Timestamp>> commitTimestampsFuture,
      Future<List<Timestamp>> readTimestampsFuture)
      throws InterruptedException, ExecutionException {
    final List<Timestamp> commitTimestamps = commitTimestampsFuture.get();
    final List<Timestamp> readTimestamps = readTimestampsFuture.get();

    System.out.println();
    for (int i = 0; i < numberOfElements; i++) {
      final Timestamp commitTimestamp = commitTimestamps.get(i);
      final Timestamp readTimestamp = readTimestamps.get(i);

      if (readTimestamp.before(commitTimestamp)) {
        System.out.println(
            RED + "[" + i + "] Read timestamp " + readTimestamp + " is before " + commitTimestamp
                + RESET);
      }
    }
  }

  private static void deleteAllFromSingers() throws SQLException {
    try (
        final Connection connection = DriverManager.getConnection(URL);
        final Statement statement = connection.createStatement()
    ) {
      statement.execute("DELETE FROM Singers WHERE TRUE");
    }
  }

  private static Callable<List<Timestamp>> consumer(BlockingQueue<Long> queue,
      long numberOfElements) {
    return () -> {
      final List<Timestamp> timestamps = new ArrayList<>();
      for (int i = 0; i < numberOfElements; i++) {
        final Long singerId = queue.poll(10, TimeUnit.SECONDS);
        try (
            final Connection connection = DriverManager.getConnection(URL);
            final PreparedStatement preparedStatement = connection
                .prepareStatement("SELECT * FROM Singers WHERE SingerId = ?")
        ) {
          preparedStatement.setLong(1, singerId);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
            }
          }
          try (
              final Statement statement = connection.createStatement();
              final ResultSet resultSet = statement.executeQuery("SHOW VARIABLE READ_TIMESTAMP")
          ) {
            if (resultSet.next()) {
              final Timestamp readTimestamp = resultSet.getTimestamp("READ_TIMESTAMP");
              timestamps.add(readTimestamp);
              System.out.println(
                  "[" + singerId + "] read timestamp is " + readTimestamp);
            }
          }
        }
      }
      return timestamps;
    };
  }

  private static Callable<List<Timestamp>> producer(BlockingQueue<Long> queue,
      long numberOfElements) {
    return () -> {
      final List<Timestamp> timestamps = new ArrayList<>();
      for (int i = 0; i < numberOfElements; i++) {
        try (
            final Connection connection = DriverManager.getConnection(URL);
            final PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (?, ?, ?)");
        ) {
          connection.setAutoCommit(false);
          final long singerId = i;
          final String firstName = "FirstName " + i;
          final String lastName = "LastName " + i;
          preparedStatement.setLong(1, singerId);
          preparedStatement.setString(2, firstName);
          preparedStatement.setString(3, lastName);
          preparedStatement.executeUpdate();
          connection.commit();
          try (
              final Statement statement = connection.createStatement();
              final ResultSet resultSet = statement.executeQuery("SHOW VARIABLE COMMIT_TIMESTAMP")
          ) {
            if (resultSet.next()) {
              final Timestamp commitTimestamp = resultSet.getTimestamp("COMMIT_TIMESTAMP");
              final long fiveHundredMillisInTheFuture = System.currentTimeMillis() + 500;
              final Timestamp futureTimestamp = new Timestamp(fiveHundredMillisInTheFuture);
              timestamps.add(commitTimestamp);
              System.out.println(
                  "[" + singerId + "] commit timestamp is " + commitTimestamp);

              if (commitTimestamp.after(futureTimestamp)) {
                System.out.println(RED + "[" + singerId
                    + "] Commit timestamp clock drift is high (commit timestamp = "
                    + commitTimestamp + ", clock.now = " + futureTimestamp + ")" + RESET);
              }
            }
          }
          queue.add(singerId);
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
      }
      return timestamps;
    };
  }

}
