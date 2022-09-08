package apoc.ttl;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Session;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertTrue;

public class TTLTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session neo4jSession;

    @BeforeClass
    public static void setupContainer() {
        // TODO We cannot build core from here anymore. This needs rethinking
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED, TestContainerUtil.ApocPackage.CORE), true)
                .withEnv( Map.of(
                        "apoc.ttl.enabled", "true",
                        "apoc.ttl.schedule", "10"));
        neo4jContainer.start();
        neo4jSession = neo4jContainer.getSession();
    }

    @After
    public void cleanDb() {
        neo4jSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }

    @Test
    public void testExpireManyNodes() {
        int fooCount = 200;
        int barCount = 300;
        neo4jSession.writeTransaction(tx -> tx.run("UNWIND range(1," + fooCount + ") as range CREATE (:Baz)-[:REL_TEST]->(n:Foo:TTL {id: range, ttl: timestamp() + 100});"));
        neo4jSession.writeTransaction(tx -> tx.run("UNWIND range(1," + barCount + ") as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});"));
        assertTrue(isNodeCountConsistent(fooCount, barCount));
        org.neo4j.test.assertion.Assert.assertEventually(() -> isNodeCountConsistent(0, 0), (value) -> value, 30L, TimeUnit.SECONDS);
    }

    // test extracted from apoc.date
    @Test
    public void testExpire() {
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100"));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (n:Bar) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"));
        assertTrue(isNodeCountConsistent(1,1));
        org.neo4j.test.assertion.Assert.assertEventually(() -> isNodeCountConsistent(0, 0), (value) -> value, 10L, TimeUnit.SECONDS);
    }

    private static boolean isNodeCountConsistent(long foo, long bar) {
        AtomicLong fooInDb = new AtomicLong();
        AtomicLong barInDb = new AtomicLong();
        AtomicLong ttlInDb = new AtomicLong();

        testCall(neo4jSession, "MATCH (n:Foo) RETURN count(n) as count", (row) -> fooInDb.set((long) row.get("count")));
        testCall(neo4jSession, "MATCH (n:Bar) RETURN count(n) as count", (row) -> barInDb.set((long) row.get("count")));
        testCall(neo4jSession, "MATCH (n:TTL) RETURN count(n) as count", (row) -> ttlInDb.set((long) row.get("count")));

        return foo == fooInDb.get()
                    && bar == barInDb.get()
                    && foo + bar == ttlInDb.get();
    }
}
