package com.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class IgniteTest {

    static class Test {
        String id;
        String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    static class TestKey {
        String id;

        public TestKey(String id) {
            this.id = id;
        }
    }

    public static void createCaches(final String[] addresses) throws Exception {
        ClientCacheConfiguration cacheCfg = getCacheCfg("Test");
        cacheCfg.setQueryEntities(getQueryEntity());

        ClientConfiguration clientCfg = new ClientConfiguration()
                .setAddresses(addresses)
                .setPartitionAwarenessEnabled(true);


        ObjectMapper mapper = new ObjectMapper();
        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            try {
                client.destroyCache(cacheCfg.getName());
            } catch (ClientException e) {
                // catch e in case the cache does not exist
                System.err.println(e.getMessage());
            }

            final ClientCache<TestKey, Test> cache = client.getOrCreateCache(cacheCfg);
            final List<String[]> data = getData();
            final String[] header = data.get(0);
            final Map<TestKey, Test> map = Maps.newLinkedHashMap();
            for (int i = 1; i < data.size(); ++i) {
                final String[] row = data.get(i);
                final Map<String, String> tmp = Maps.newLinkedHashMap();
                IntStream.range(0, row.length).boxed().forEach(j -> tmp.put(header[j], row[j]));

                final Test value = mapper.convertValue(tmp, Test.class);
                final TestKey key = new TestKey(value.id);
                map.put(key, value);
            }
            cache.putAll(map);
        }
    }

    private static List<String[]> getData() {
        return ImmutableList.<String[]>builder()
                .add(new String[] { "id", "name" })
                .add(new String[] { "1", "alice" })
                .add(new String[] { "2", "bob" })
                .add(new String[] { "3", "carol" })
                .add(new String[] { "4", "dave" })
                .add(new String[] { "5", "eve" })
                .build();
    }

    private static ClientCacheConfiguration getCacheCfg(final String cacheName) {
        ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration();
        cacheCfg.setName(cacheName);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(0);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.IGNORE);
        cacheCfg.setExpiryPolicy(new AccessedExpiryPolicy(new Duration(TimeUnit.DAYS, 7)));
        return cacheCfg;
    }

    private static QueryEntity getQueryEntity() {
        return new QueryEntity(TestKey.class, Test.class)
                .addQueryField("id", String.class.getName(), null)
                .addQueryField("name", String.class.getName(), null)
                .setIndexes(Lists.newArrayList(new QueryIndex("id"), new QueryIndex("name")));
    }

    public static void main(String[] args) throws Exception {
        final String[] addresses = {"localhost:10800", "localhost:10801", "localhost:10802"};
        createCaches(addresses);
    }
}
