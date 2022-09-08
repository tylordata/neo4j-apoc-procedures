package apoc.uuid;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class UUIDTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session dbWithoutApocPeriodicSession;
    private static final String DB_TEST = "dbWithoutApocPeriodic";
    private static final String UUID_TEST_REGEXP = "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

    @BeforeClass
    public static void setupContainer() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withNeo4jConfig("dbms.security.auth_enabled", "true")
                .withEnv(APOC_UUID_ENABLED, "true");
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();
        createDatabases();
        createSessions();

        //TestUtil.registerProcedure(db, Uuid.class, Create.class, Periodic.class);
        //TestUtil.registerProcedure(dbWithoutApocPeriodic, Uuid.class, Create.class);
    }

    @After
    public void cleanDb() {
        neo4jSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        neo4jSession.writeTransaction(tx -> tx.run("DROP CONSTRAINT constraint IF EXISTS"));
        dbWithoutApocPeriodicSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        dbWithoutApocPeriodicSession.writeTransaction(tx -> tx.run("DROP CONSTRAINT constraint IF EXISTS"));
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }

    @Test
    public void testUUID() {
        // given
        neo4jSession.writeTransaction(tx -> tx.run("CREATE CONSTRAINT constraint FOR (p:Person) REQUIRE p.uuid IS UNIQUE"));
        neo4jSession.writeTransaction(tx -> {
            tx.run("CALL apoc.uuid.install('Person') YIELD label RETURN label");
            return tx.run("CREATE (p:Person{name:'Daniel'})-[:WORK]->(c:Company{name:'Neo4j'})");
        });

        // then
        var hasUUID = fetchUUID("MATCH (c:Company) return c.uuid AS uuid");
         //assertFalse( "UUID not set on node after 5 seconds", hasUUID );


        var hasUUID2 = fetchUUID("MATCH (p:Person) return p.uuid AS uuid");
         //assertTrue( "UUID not set on node after 5 seconds", hasUUID2 );
        //try (Transaction tx = db.beginTx()) {
            //Node company = (Node) tx.execute("MATCH (c:Company) return c").next().get("c");
            //assertTrue(!company.hasProperty("uuid"));
            //Node person = (Node) tx.execute("MATCH (p:Person) return p").next().get("p");
            //assertTrue(person.getAllProperties().containsKey("uuid"));

            //assertTrue(person.getAllProperties().get("uuid").toString().matches(UUID_TEST_REGEXP));
          //  tx.commit();
        //}

    }

//    @Test
//    public void testUUIDWithSetLabel() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (p:Mario) REQUIRE p.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Mario', {addToSetLabels: true}) YIELD label RETURN label");
//        // when
//        db.executeTransactionally("CREATE (p:Luigi {foo:'bar'}) SET p:Mario");
//        // then
//        TestUtil.testCall(db, "MATCH (a:Luigi:Mario) RETURN a.uuid as uuid",
//                row -> assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));
//
//        // - set after creation
//        db.executeTransactionally("CREATE (:Peach)");
//        // when
//        db.executeTransactionally("MATCH (p:Peach) SET p:Mario");
//        // then
//        TestUtil.testCall(db, "MATCH (a:Peach:Mario) RETURN a.uuid as uuid",
//                row -> assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));
//
//        TestUtil.testCall(db, "CALL apoc.uuid.remove('Mario')",
//                (row) -> assertResult(row, "Mario", false,
//                        Util.map("uuidProperty", "uuid", "addToSetLabels", true)));
//    }
//
//    @Test
//    public void testUUIDWithoutRemovedUuid() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (test:Test) REQUIRE test.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
//
//        // when
//        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})"); // Create the uuid manually and except is the same after the trigger
//
//        // then
//        try (Transaction tx = db.beginTx()) {
//            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
//            assertTrue(n.getAllProperties().containsKey("uuid"));
//            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid")); // Check if the uuid if the same when created
//            tx.commit();
//        }
//    }
//
//    @Test
//    public void testUUIDSetUuidToEmptyAndRestore() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (test:Test) REQUIRE test.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
//        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");
//
//        // when
//        db.executeTransactionally("MATCH (t:Test) SET t.uuid = ''");
//
//        // then
//        try (Transaction tx = db.beginTx()) {
//            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
//            assertTrue(n.getAllProperties().containsKey("uuid"));
//            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
//            tx.commit();
//        }
//    }
//
//    @Test
//    public void testUUIDDeleteUuidAndRestore() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (test:Test) REQUIRE test.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
//        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");
//
//        // when
//        db.executeTransactionally("MATCH (t:Test) remove t.uuid");
//
//        // then
//        try (Transaction tx = db.beginTx()) {
//            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
//            assertTrue(n.getAllProperties().containsKey("uuid"));
//            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
//            tx.commit();
//        }
//    }
//
//    @Test
//    public void testUUIDSetUuidToEmpty() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (test:Test) REQUIRE test.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
//        db.executeTransactionally("CREATE (n:Test:Empty {name:'empty'})");
//
//        // when
//        db.executeTransactionally("MATCH (t:Test:Empty) SET t.uuid = ''");
//
//        // then
//        try (Transaction tx = db.beginTx()) {
//            Node n = (Node) tx.execute("MATCH (n:Empty) return n").next().get("n");
//            assertTrue(n.getAllProperties().containsKey("uuid"));
//            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
//            tx.commit();
//        }
//    }
//
//    @Test
//    public void testUUIDList() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (bar:Bar) REQUIRE bar.uuid IS UNIQUE");
//
//        // when
//        db.executeTransactionally("CALL apoc.uuid.install('Bar') YIELD label RETURN label");
//
//        // then
//        TestUtil.testCall(db, "CALL apoc.uuid.list()",
//                (row) -> assertResult(row, "Bar", true,
//                        Util.map("uuidProperty", "uuid", "addToSetLabels", false)));
//    }
//
//    @Test
//    public void testUUIDListAddToExistingNodes() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (bar:Bar) REQUIRE bar.uuid IS UNIQUE");
//        db.executeTransactionally("UNWIND Range(1,10) as i CREATE(bar:Bar{id: i})");
//
//        // when
//        db.executeTransactionally("CALL apoc.uuid.install('Bar')");
//
//        // then
//        List<String> uuidList = TestUtil.firstColumn(db, "MATCH (n:Bar) RETURN n.uuid AS uuid");
//        assertEquals(10, uuidList.size());
//        assertTrue(uuidList.stream().allMatch(uuid -> uuid.matches(UUID_TEST_REGEXP)));
//    }
//
//    @Test
//    public void testAddRemoveUuid() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (test:Test) REQUIRE test.foo IS UNIQUE");
//
//        // when
//        db.executeTransactionally("CALL apoc.uuid.install('Test', {uuidProperty: 'foo'}) YIELD label RETURN label");
//
//        // then
//        TestUtil.testCall(db, "CALL apoc.uuid.list()",
//                (row) -> assertResult(row, "Test", true,
//                        Util.map("uuidProperty", "foo", "addToSetLabels", false)));
//        TestUtil.testCall(db, "CALL apoc.uuid.remove('Test')",
//                (row) -> assertResult(row, "Test", false,
//                        Util.map("uuidProperty", "foo", "addToSetLabels", false)));
//    }
//
    @Test
    public void testNotAddToExistingNodes() {
        // given
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})"));

        // when
        neo4jSession.writeTransaction(tx -> tx.run("CREATE CONSTRAINT constraint FOR (person:Person) REQUIRE person.uuid IS UNIQUE"));
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.uuid.install('Person', {addToExistingNodes: false}) YIELD label RETURN label"));

        // then
        assertFalse(nodeHasUUID("MATCH (person:Person) return person"));
    }
//
//    @Test
//    public void testAddToExistingNodes() {
//        // given
//        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");
//
//        // when
//        db.executeTransactionally("CREATE CONSTRAINT FOR (person:Person) REQUIRE person.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Person') YIELD label RETURN label");
//
//        // then
//        try (Transaction tx = db.beginTx()) {
//            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
//            assertTrue(n.getAllProperties().containsKey("uuid"));
//            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
//            tx.commit();
//        }
//    }
//
//    @Test
//    public void testAddToExistingNodesBatchResult() {
//        // given
//        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");
//
//        // when
//        db.executeTransactionally("CREATE CONSTRAINT FOR (person:Person) REQUIRE person.uuid IS UNIQUE");
//
//        // then
//        try (Transaction tx = db.beginTx()) {
//            long total = (Long) tx.execute(
//                    "CALL apoc.uuid.install('Person') YIELD label, installed, properties, batchComputationResult " +
//                            "RETURN batchComputationResult.total as total")
//                    .next()
//                    .get("total");
//            assertEquals(1, total);
//            tx.commit();
//        }
//    }
//
//    @Test
//    public void testRemoveAllUuid() {
//        // given
//        db.executeTransactionally("CREATE CONSTRAINT FOR (test:Test) REQUIRE test.foo IS UNIQUE");
//        db.executeTransactionally("CREATE CONSTRAINT FOR (bar:Bar) REQUIRE bar.uuid IS UNIQUE");
//        db.executeTransactionally("CALL apoc.uuid.install('Bar') YIELD label RETURN label");
//        db.executeTransactionally("CALL apoc.uuid.install('Test', {addToExistingNodes: false, uuidProperty: 'foo'}) YIELD label RETURN label");
//
//        // when
//        TestUtil.testResult(db, "CALL apoc.uuid.removeAll()",
//                (result) -> {
//                    // then
//                    Map<String, Object> row = result.next();
//                    assertResult(row, "Test", false,
//                            Util.map("uuidProperty", "foo", "addToSetLabels", false));
//                    row = result.next();
//                    assertResult(row, "Bar", false,
//                            Util.map("uuidProperty", "uuid", "addToSetLabels", false));
//                });
//    }
//

    @Ignore
    @Test(expected = RuntimeException.class)
    public void testAddWithError() {
        try {
            // when
            neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.uuid.install('Wrong') YIELD label RETURN label"));
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT FOR (wrong:Wrong) REQUIRE wrong.uuid IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

//    @Test(expected = RuntimeException.class)
//    public void testAddWithErrorAndCustomField() {
//        try {
//            // when
//            db.executeTransactionally("CALL apoc.uuid.install('Wrong', {uuidProperty: 'foo'}) YIELD label RETURN label");
//        } catch (RuntimeException e) {
//            // then
//            Throwable except = ExceptionUtils.getRootCause(e);
//            assertTrue(except instanceof RuntimeException);
//            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT FOR (wrong:Wrong) REQUIRE wrong.foo IS UNIQUE`", except.getMessage());
//            throw e;
//        }
//    }
//
//    @Test(expected = RuntimeException.class)
//    public void testAddToAllExistingNodesIfCoreNotInstalled() {
//        try {
//            // when
//            dbWithoutApocPeriodic.executeTransactionally("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE");
//            dbWithoutApocPeriodic.executeTransactionally("CALL apoc.uuid.install('Person') YIELD label RETURN label");
//        } catch (RuntimeException e) {
//            // then
//            Throwable except = ExceptionUtils.getRootCause(e);
//            assertTrue(except instanceof RuntimeException);
//            assertEquals("apoc core needs to be installed when using apoc.uuid.install with the flag addToExistingNodes = true", except.getMessage());
//            throw e;
//        }
//    }

    private void assertResult(Map<String, Object> row, String labels, boolean installed, Map<String, Object> conf) {
        assertEquals(labels, row.get("label"));
        assertEquals(installed, row.get("installed"));
        assertEquals(conf, row.get("properties"));
    }

    private static void createDatabases() {
        try (Session systemSession = driver.session( SessionConfig.forDatabase("system"))) {
            systemSession.writeTransaction(tx -> tx.run(String.format("CREATE DATABASE %s WAIT;", DB_TEST)));
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        dbWithoutApocPeriodicSession = driver.session(SessionConfig.forDatabase(DB_TEST));
    }


    private static String fetchUUID(String call)
    {
        AtomicReference<String> uuid = new AtomicReference<>();
        AtomicBoolean nodeHasUUID = new AtomicBoolean(false);
        Consumer<Iterator<Map<String,Object>>> resultConsumer = (result) ->
        {
            Map<String,Object> r = result.next();
            boolean hasUUID = r.get( "uuid" ) != null;
            nodeHasUUID.set(hasUUID);
            if (hasUUID) {
                uuid.set((String)r.get("uuid"));
            }
        };

        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 5 );
        while ( System.currentTimeMillis() < timeout && !nodeHasUUID.get() )
        {
            neo4jSession.writeTransaction( tx ->
            {
                Map<String,Object> p = Collections.<String,Object>emptyMap();
                resultConsumer.accept( tx.run( call, p ).list().stream().map( org.neo4j.driver.Record::asMap ).collect( Collectors.toList() ).iterator() );
                tx.commit();
                return null;
            } );
        }
       return uuid.get();
    }

    private static boolean nodeHasUUID(String call)
    {
        AtomicBoolean nodeHasUUID = new AtomicBoolean(false);
        Consumer<Iterator<Map<String,Object>>> resultConsumer = (result) ->
        {
            Map<String,Object> r = result.next();
            nodeHasUUID.set(r.get( "uuid" ) != null);
        };

        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 5 );
        while ( System.currentTimeMillis() < timeout && !nodeHasUUID.get() )
        {
            neo4jSession.writeTransaction( tx ->
            {
                Map<String,Object> p = Collections.<String,Object>emptyMap();
                resultConsumer.accept( tx.run( call, p ).list().stream().map( org.neo4j.driver.Record::asMap ).collect( Collectors.toList() ).iterator() );
                tx.commit();
                return null;
            } );
        }
       return nodeHasUUID.get();
    }
}
