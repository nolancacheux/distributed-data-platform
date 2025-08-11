import pg from "pg";
import { createClient } from "redis";
import neo4j from "neo4j-driver";
import cassandra from "cassandra-driver";

async function testPostgres() {
  const client = new pg.Client({
    host: process.env.POSTGRES_HOST,
    port: 5432,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
    ssl: {
      rejectUnauthorized: false, // accept self-signed certs (fine for test/dev)
    },
  });
  await client.connect();
  console.log("✅ PostgreSQL OK");
  await client.end();
}

async function testKeyDB() {
  const client = createClient({
    socket: {
      host: process.env.KEYDB_HOST,
      port: parseInt(process.env.KEYDB_PORT || "6379"),
    },
    password: process.env.KEYDB_PASSWORD || undefined,
  });
  await client.connect();
  console.log("✅ KeyDB OK");
  await client.quit();
}

async function testNeo4j() {
  const driver = neo4j.driver(
    `bolt://${process.env.NEO4J_HOST}:7687`,
    neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD),
  );
  const session = driver.session();
  await session.run("RETURN 1");
  console.log("✅ Neo4j OK");
  await session.close();
  await driver.close();
}

async function testCassandra() {
  const client = new cassandra.Client({
    contactPoints: [process.env.CASSANDRA_HOST],
    localDataCenter: "dc1",
    keyspace: process.env.CASSANDRA_KEYSPACE,
    authProvider: new cassandra.auth.PlainTextAuthProvider(
      process.env.CASSANDRA_USER,
      process.env.CASSANDRA_PASSWORD,
    ),
  });
  await client.connect();
  console.log("✅ Cassandra OK");
  await client.shutdown();
}

import crypto from "crypto";

function hashConfig(config) {
  console.log(`🧾 Config String: ${config}`);
  const hash = crypto.createHash("sha256");
  hash.update(config);
  return hash.digest("hex").slice(0, 10); // Shortened hash for clarity
}

const tests = [
  { name: "PostgreSQL", fn: testPostgres },
  { name: "KeyDB", fn: testKeyDB },
  { name: "Neo4j", fn: testNeo4j },
  { name: "Cassandra", fn: testCassandra },
];

(async () => {
  const configHash = hashConfig(
    [
      process.env.POSTGRES_HOST,
      process.env.POSTGRES_DB,
      process.env.POSTGRES_USER,
      process.env.KEYDB_HOST,
      process.env.NEO4J_HOST,
      process.env.NEO4J_USER,
      process.env.NEO4J_PASSWORD,
      process.env.CASSANDRA_HOST,
      process.env.CASSANDRA_KEYSPACE,
      process.env.CASSANDRA_USER,
      process.env.CASSANDRA_PASSWORD,
    ].join("|"),
  );

  console.log(`🧾 Config Hash: ${configHash}`);

  let success = true;

  for (const { name, fn } of tests) {
    try {
      await fn();
    } catch (err) {
      console.error(`❌ ${name} failed:`, err.message);
      success = false;
    }
  }

  if (!success) {
    process.exit(1);
  } else {
    console.log("🎉 All database checks passed!");
  }
})();
