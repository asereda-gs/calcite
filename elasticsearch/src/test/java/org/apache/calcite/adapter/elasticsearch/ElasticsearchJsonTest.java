

package org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class ElasticsearchJsonTest {

  private  ObjectMapper mapper;

  @Before
  public void setUp() throws Exception {
    ObjectMapper mapper = new ObjectMapper()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    this.mapper = mapper;
  }

  @Test
  public void agg_empty() throws Exception {
    String json = "{}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertThat(a.asList().size(), is(0));
    assertThat(a.asMap().size(), is(0));
  }

  @Test
  public void agg_single1() throws Exception {
    String json = "{agg1: {value: '111'}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertEquals(1, a.asList().size());
    assertEquals(1, a.asMap().size());
    assertEquals("agg1", a.asList().get(0).getName());
    assertEquals("agg1", a.asMap().keySet().iterator().next());
    assertEquals("111", ((ElasticsearchJson.MultiValue) a.asList().get(0)).value());

    List<Map<String, Object>> rows = new ArrayList<>();
    ElasticsearchJson.visitValueNodes(a, rows::add);
    assertThat(rows.size(), is(1));
    assertThat(rows.get(0).get("agg1"), is("111"));
  }

  @Test
  public void agg_multiValues() throws Exception {
    String json = "{ agg1: {min: 0, max: 2, avg: 2.33}}";
    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertEquals(1, a.asList().size());
    assertEquals(1, a.asMap().size());
    assertEquals("agg1", a.asList().get(0).getName());

    Map<String, Object> values = ((ElasticsearchJson.MultiValue) a.get("agg1")).values();
    assertThat(values.keySet(), hasItems("min", "max", "avg"));
  }

  @Test
  public void agg_single2() throws Exception {
    String json = "{ agg1: {value: 'foo'}, agg2: {value: 42}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertEquals(2, a.asList().size());
    assertEquals(2, a.asMap().size());
    assertThat(a.asMap().keySet(), hasItems("agg1", "agg2"));
  }

  @Test
  public void agg_buckets1() throws Exception {
    String json = "{ groupby: {buckets: [{key:'k1', doc_count:0, myagg:{value: 1.1}},"
        + " {key:'k2', myagg:{value: 2.2}}] }}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);

    assertThat(a.asMap().keySet(), hasItem("groupby"));
    assertThat(a.get("groupby"), instanceOf(ElasticsearchJson.MultiBucketsAggregation.class));
    ElasticsearchJson.MultiBucketsAggregation multi = a.get("groupby");
    assertThat(multi.buckets().size(), is(2));
    assertThat(multi.getName(), is("groupby"));
    assertThat(multi.buckets().get(0).key(), is("k1"));
    assertThat(multi.buckets().get(0).keyAsString(), is("k1"));
    assertThat(multi.buckets().get(1).key(), is("k2"));
    assertThat(multi.buckets().get(1).keyAsString(), is("k2"));
  }

  @Test
  public void agg_manyAggregations() throws Exception {
    String json = "{groupby:{buckets:["
        + "{key:'k1', a1:{value:1}, a2:{value:2}},"
        + "{key:'k2', a1:{value:3}, a2:{value:4}}"
        + "]}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    ElasticsearchJson.MultiBucketsAggregation multi = a.get("groupby");

    assertThat(multi.buckets().get(0).getAggregations().asMap().size(), is(2));
    assertThat(multi.buckets().get(0).getName(), is("groupby"));
    assertThat(multi.buckets().get(0).key(), is("k1"));
    assertThat(multi.buckets().get(0).getAggregations().asMap().keySet(), hasItems("a1", "a2"));
    assertThat(multi.buckets().get(1).getAggregations().asMap().size(), is(2));
    assertThat(multi.buckets().get(1).getName(), is("groupby"));
    assertThat(multi.buckets().get(1).key(), is("k2"));
    assertThat(multi.buckets().get(1).getAggregations().asMap().keySet(), hasItems("a1", "a2"));
    List<Map<String, Object>> rows = new ArrayList<>();
    ElasticsearchJson.visitValueNodes(a, rows::add);
    assertThat(rows.size(), is(2));
    assertThat(rows.get(0).get("groupby"), is("k1"));
    assertThat(rows.get(0).get("a1"), is(1));
    assertThat(rows.get(0).get("a2"), is(2));
  }

  @Test
  public void agg_multiBuckets() throws Exception {
    String json = "{col1: {buckets: ["
        + "{col2: {doc_count:1, buckets:[{key:'k3', max:{value:41}}]}, key:'k1'},"
        + "{col2: {buckets:[{key:'k4', max:{value:42}}], doc_count:1}, key:'k2'}"
        + "]}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);

    assertThat(a.asMap().keySet(), hasItem("col1"));
    assertThat(a.get("col1"), instanceOf(ElasticsearchJson.MultiBucketsAggregation.class));
    ElasticsearchJson.MultiBucketsAggregation m = a.get("col1");
    assertThat(m.getName(), is("col1"));
    assertThat(m.buckets().size(), is(2));
    assertThat(m.buckets().get(0).key(), is("k1"));
    assertThat(m.buckets().get(0).getName(), is("col1"));
    assertThat(m.buckets().get(0).getAggregations().asMap().keySet(), hasItem("col2"));
    assertThat(m.buckets().get(1).key(), is("k2"));
    List<Map<String, Object>> rows = new ArrayList<>();
    ElasticsearchJson.visitValueNodes(a, rows::add);
    assertThat(rows.size(), is(2));

    assertThat(rows.get(0).keySet(), hasItems("col1", "col2", "max"));
    assertThat(rows.get(0).get("col1"), is("k1"));
    assertThat(rows.get(0).get("col2"), is("k3"));
    assertThat(rows.get(0).get("max"), is(41));

    assertThat(rows.get(1).keySet(), hasItems("col1", "col2", "max"));
    assertThat(rows.get(1).get("col1"), is("k2"));
    assertThat(rows.get(1).get("col2"), is("k4"));
    assertThat(rows.get(1).get("max"), is(42));

  }

}