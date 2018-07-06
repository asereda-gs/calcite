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
package org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * Internal objects (and deserializers) used to parse elastic search results
 * (which are in JSON format).
 *
 * <p>Since we're using basic row-level rest client http response has to be
 * processed manually using JSON (jackson) library.
 */
class ElasticsearchJson {

  private ElasticsearchJson() {}

  /**
   * Visits leaves of the aggregation.
   */
  static void visitValueNodes(Aggregations aggregations, Consumer<Map<String, Object>> consumer) {
    Objects.requireNonNull(aggregations, "aggregations");
    Objects.requireNonNull(consumer, "consumer");

    List<Bucket> buckets = new ArrayList<>();

    Map<RowKey, List<MultiValue>> rows = new LinkedHashMap<>();

    BiConsumer<RowKey, MultiValue> cons = (r, v) -> rows.computeIfAbsent(r, ignore -> new ArrayList<>()).add(v);
    aggregations.forEach(a -> visitValueNodes(a, buckets, cons));
    rows.forEach((k, v) -> {
      Map<String, Object> row = new LinkedHashMap<>(k.keys);
      v.forEach(val -> row.put(val.getName(), val.value()));
      consumer.accept(row);
    });
  }

  private static class RowKey {
    private final Map<String, Object> keys;
    private final int hashCode;

    private RowKey(final Map<String, Object> keys) {
      this.keys = Objects.requireNonNull(keys, "keys");
      this.hashCode = Objects.hashCode(keys);
    }

    private RowKey(List<Bucket> buckets) {
      this(buckets.stream().collect(Collectors
          .toMap(Bucket::getName, Bucket::key)));
    }

    @Override public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RowKey rowKey = (RowKey) o;
      return hashCode == rowKey.hashCode
          && Objects.equals(keys, rowKey.keys);
    }

    @Override public int hashCode() {
      return this.hashCode;
    }
  }

  private static void visitValueNodes(Aggregation aggregation, List<Bucket> parents,
      BiConsumer<RowKey, MultiValue> consumer) {

    if (aggregation instanceof MultiValue) {
      // publish one value of the row
      RowKey key = new RowKey(parents);
      consumer.accept(key, (MultiValue) aggregation);
      return;
    }

    if (aggregation instanceof Bucket) {
      Bucket bucket = (Bucket) aggregation;
      parents.add(bucket);
      bucket.getAggregations().forEach(a -> visitValueNodes(a, parents, consumer));
      parents.remove(parents.size() - 1);
    } else if (aggregation instanceof HasAggregations) {
      HasAggregations children = (HasAggregations) aggregation;
      children.getAggregations().forEach(a -> visitValueNodes(a, parents, consumer));
    } else if (aggregation instanceof MultiBucketsAggregation) {
      MultiBucketsAggregation multi = (MultiBucketsAggregation) aggregation;
      multi.buckets().forEach(b -> {
        parents.add(b);
        b.getAggregations().forEach(a -> visitValueNodes(a, parents, consumer));
        parents.remove(parents.size() - 1);
      });
    }

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Result {
    private final SearchHits hits;
    private final Aggregations aggregations;
    private final long took;

    /**
     * Constructor for this instance.
     * @param hits list of matched documents
     * @param took time taken (in took) for this query to execute
     */
    @JsonCreator
    Result(@JsonProperty("hits") SearchHits hits,
        @JsonProperty("aggregations") Aggregations aggregations,
        @JsonProperty("took") long took) {
      this.hits = Objects.requireNonNull(hits, "hits");
      this.aggregations = aggregations;
      this.took = took;
    }

    SearchHits searchHits() {
      return hits;
    }

    Aggregations aggregations() {
      return aggregations;
    }

    public Duration took() {
      return Duration.ofMillis(took);
    }

  }

  /**
   * Similar to {@code SearchHits} in ES. Container for {@link SearchHit}
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SearchHits {

    private final long total;
    private final List<SearchHit> hits;

    @JsonCreator
    SearchHits(@JsonProperty("total")final long total,
               @JsonProperty("hits") final List<SearchHit> hits) {
      this.total = total;
      this.hits = Objects.requireNonNull(hits, "hits");
    }

    public List<SearchHit> hits() {
      return this.hits;
    }

    public long total() {
      return total;
    }

  }

  /**
   * Concrete result record which matched the query. Similar to {@code SearchHit} in ES.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SearchHit {
    private final String id;
    private final Map<String, Object> source;
    private final Map<String, Object> fields;

    @JsonCreator
    SearchHit(@JsonProperty("_id") final String id,
                      @JsonProperty("_source") final Map<String, Object> source,
                      @JsonProperty("fields") final Map<String, Object> fields) {
      this.id = Objects.requireNonNull(id, "id");

      // both can't be null
      if (source == null && fields == null) {
        final String message = String.format(Locale.ROOT,
            "Both '_source' and 'fields' are missing for %s", id);
        throw new IllegalArgumentException(message);
      }

      // both can't be non-null
      if (source != null && fields != null) {
        final String message = String.format(Locale.ROOT,
            "Both '_source' and 'fields' are populated (non-null) for %s", id);
        throw new IllegalArgumentException(message);
      }

      this.source = source;
      this.fields = fields;
    }

    /**
     * Returns id of this hit (usually document id)
     * @return unique id
     */
    public String id() {
      return id;
    }

    /**
     * Finds specific attribute from ES search result
     * @param name attribute name
     * @return value from result (_source or fields)
     */
    Object value(String name) {
      Objects.requireNonNull(name, "name");

      if (!sourceOrFields().containsKey(name)) {
        final String message = String.format(Locale.ROOT,
            "Attribute '%s' not found in search result '%s'", name, id);
        throw new IllegalArgumentException(message);
      }

      if (source != null) {
        return source.get(name);
      } else if (fields != null) {
        Object field = fields.get(name);
        if (field instanceof Iterable) {
          // return first element (or null)
          Iterator<?> iter = ((Iterable<?>) field).iterator();
          return iter.hasNext() ? iter.next() : null;
        }

        return field;
      }

      throw new AssertionError("Shouldn't get here: " + id);

    }

    Map<String, Object> source() {
      return source;
    }

    Map<String, Object> fields() {
      return fields;
    }

    Map<String, Object> sourceOrFields() {
      return source != null ? source : fields;
    }
  }


  @JsonDeserialize(using = AggregationsSerializer.class)
  static class Aggregations implements Iterable<Aggregation> {

    private final List<? extends Aggregation> aggregations;
    private Map<String, Aggregation> aggregationsAsMap;

    Aggregations(List<? extends Aggregation> aggregations) {
      this.aggregations = Objects.requireNonNull(aggregations, "aggregations");
    }

    /**
     * Iterates over the {@link Aggregation}s.
     */
    @Override
    public final Iterator<Aggregation> iterator() {
      return asList().iterator();
    }

    /**
     * The list of {@link Aggregation}s.
     */
    final List<Aggregation> asList() {
      return Collections.unmodifiableList(aggregations);
    }

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name. Lazy init.
     */
    final Map<String, Aggregation> asMap() {
      if (aggregationsAsMap == null) {
        Map<String, Aggregation> map = new LinkedHashMap<>(aggregations.size());
        for (Aggregation aggregation : aggregations) {
          map.put(aggregation.getName(), aggregation);
        }
        this.aggregationsAsMap = unmodifiableMap(map);
      }
      return aggregationsAsMap;
    }

    /**
     * Returns the aggregation that is associated with the specified name.
     */
    @SuppressWarnings("unchecked")
    public final <A extends Aggregation> A get(String name) {
      return (A) asMap().get(name);
    }

    @Override
    public final boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      return aggregations.equals(((Aggregations) obj).aggregations);
    }

    @Override
    public final int hashCode() {
      return Objects.hash(getClass(), aggregations);
    }

  }


  interface Aggregation {

    /**
     * @return The name of this aggregation.
     */
    String getName();

  }

  interface HasAggregations {
    Aggregations getAggregations();
  }

  /**
   * An aggregation that returns multiple buckets
   */
  static class MultiBucketsAggregation implements Aggregation {

    private final String name;
    private final List<Bucket> buckets;

    MultiBucketsAggregation(final String name,
        final List<Bucket> buckets) {
      this.name = name;
      this.buckets = buckets;
    }

    /**
     * @return  The buckets of this aggregation.
     */
    List<Bucket> buckets() {
      return buckets;
    }

    @Override public String getName() {
      return name;
    }
  }

  /**
   * A bucket represents a criteria to which all documents that fall in it adhere to. It is also uniquely identified
   * by a key, and can potentially hold sub-aggregations computed over all documents in it.
   */
  static class Bucket implements HasAggregations, Aggregation {
    private final Object key;
    private final String name;
    private final Aggregations aggregations;

    Bucket(final Object key,
        final String name,
        final Aggregations aggregations) {
      this.key = key; // key can be set after construction
      this.name = Objects.requireNonNull(name, "name");
      this.aggregations = Objects.requireNonNull(aggregations, "aggregations");
    }

    /**
     * @return The key associated with the bucket
     */
    Object key() {
      return key;
    }

    /**
     * @return The key associated with the bucket as a string
     */
    String keyAsString() {
      return Objects.toString(key());
    }

    /**
     * @return  The sub-aggregations of this bucket
     */
    @Override
    public Aggregations getAggregations() {
      return aggregations;
    }

    @Override public String getName() {
      return name;
    }
  }

  static class MultiValue implements Aggregation {
    private final String name;
    private final Map<String, Object> values;

    MultiValue(final String name, final Map<String, Object> values) {
      this.name = Objects.requireNonNull(name, "name");
      this.values = Objects.requireNonNull(values, "values");
    }

    @Override public String getName() {
      return name;
    }

    Map<String, Object> values() {
      return values;
    }

    /**
     * For single value. Returns single value represented by this leaf aggregation.
     * @return value corresponding to {@code value}
     */
    Object value() {
      if (!values().containsKey("value")) {
        throw new IllegalStateException("'value' field not present in this aggregation");
      }

      return values().get("value");
    }

  }

  static class AggregationsSerializer extends StdDeserializer<Aggregations> {

    private static final Set<String> IGNORE_TOKENS = new HashSet<>(Arrays.asList("meta",
        "buckets", "value", "values", "value_as_string", "doc_count", "key", "key_as_string"));

    AggregationsSerializer() {
      super(Aggregations.class);
    }

    @Override public Aggregations deserialize(final JsonParser parser,
        final DeserializationContext ctxt)
        throws IOException  {

      JsonNode node = parser.getCodec().readTree(parser);
      return parseAggregations(parser, (ObjectNode) node);
    }

    private static Aggregations parseAggregations(JsonParser parser, ObjectNode node)
        throws JsonProcessingException {

      List<Aggregation> aggregations = new ArrayList<>();

      Iterable<Map.Entry<String, JsonNode>> iter = node::fields;
      for (Map.Entry<String, JsonNode> entry : iter) {
        final String name = entry.getKey();
        final JsonNode value = entry.getValue();

        Aggregation agg = null;
        if (value.has("buckets")) {
          agg = parseBuckets(parser, name, (ArrayNode) value.get("buckets"));
        } else if (value.isObject() && !IGNORE_TOKENS.contains(name)) {
          // leaf
          agg = parseValue(parser, name, (ObjectNode) value);
        }

        if (agg != null) {
          aggregations.add(agg);
        }
      }

      return new Aggregations(aggregations);
    }

    private static MultiValue parseValue(JsonParser parser, String name, ObjectNode node)
        throws JsonProcessingException {

      return new MultiValue(name,
          parser.getCodec().treeToValue(node, Map.class));
    }

    private static Aggregation parseBuckets(JsonParser parser, String name, ArrayNode nodes)
      throws JsonProcessingException {

      List<Bucket> buckets = new ArrayList<>(nodes.size());
      for (JsonNode b: nodes) {
        buckets.add(parseBucket(parser, name, (ObjectNode) b));
      }

      return new MultiBucketsAggregation(name, buckets);
    }

    private static Bucket parseBucket(JsonParser parser, String name, ObjectNode node)
        throws JsonProcessingException  {
      JsonNode keyNode = node.get("key");
      final Object key;
      if (keyNode.isTextual()) {
        key = keyNode.textValue();
      } else if (keyNode.isNumber()) {
        key = keyNode.numberValue();
      } else if (keyNode.isBoolean()) {
        key = keyNode.booleanValue();
      } else {
        key = parser.getCodec().treeToValue(node, Map.class);
      }

      return new Bucket(key, name, parseAggregations(parser, node));
    }

  }
}

// End ElasticsearchSearchResult.java
