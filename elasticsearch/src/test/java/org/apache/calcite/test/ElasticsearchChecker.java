/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Internal util methods for ElasticSearch tests
 */
public class ElasticsearchChecker {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  private ElasticsearchChecker() {}


  /** Returns a function that checks that a particular Elasticsearch pipeline is
   * generated to implement a query.
   * @param strings expected expressions
   * @return validation function
   */
  public static Consumer<List> elasticsearchChecker(final String... strings) {
    Objects.requireNonNull(strings, "strings");
    return a -> {
      ObjectNode actual = a == null || a.isEmpty() ? null
            : ((ObjectNode) a.get(0));

      try {

        String json = "{" + Arrays.stream(strings).collect(Collectors.joining(",")) + "}";
        ObjectNode expected = (ObjectNode) MAPPER.readTree(json);

        if (!expected.equals(actual)) {
          assertEquals("expected and actual Elasticsearch queries do not match",
              MAPPER.writeValueAsString(expected),
              MAPPER.writeValueAsString(actual));
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

}

// End ElasticsearchChecker.java
