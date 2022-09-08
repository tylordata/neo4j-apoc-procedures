package apoc.trigger;

import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.types.Node;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class TriggerClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() throws Exception {
       cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), MapUtil.stringMap(
                        "apoc.trigger.refresh", "100",
                        "apoc.trigger.enabled", "true"
                ));
       //assertTrue(cluster.isRunning());
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Before
    public void before() {
        cluster.getSession().run("CALL apoc.trigger.removeAll()");
        cluster.getSession().run("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testReplication() throws Exception {
        cluster.getSession().run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        // Test that the trigger is present in another instance
        org.neo4j.test.assertion.Assert.assertEventually(() -> cluster.getDriver().session()
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list() YIELD name RETURN name").single().get("name").asString()),
                (value) -> "timestamp".equals(value), 30, TimeUnit.SECONDS);
    }
}
