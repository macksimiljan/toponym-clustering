package database;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.slf4j.Slf4jLogProvider;

/**
 * Manages the connection to the database.
 * 
 * @author MM
 *
 */
public abstract class DatabaseAccess {

	/** Path to the database in the file system. */
	private static final String DB_PATH = "neo4j_db";

	/** The graph database object. */
	private static GraphDatabaseService graphDb;

	/**
	 * Returns the graph database.
	 * 
	 * @return The main access point for the graph database.
	 */
	public static GraphDatabaseService getGraphDb() {
		// initializes only once for the same database
		if (graphDb == null) {
			graphDb = new GraphDatabaseFactory()
					.setUserLogProvider(new Slf4jLogProvider()) // use SLF4J for log output
					.newEmbeddedDatabase(new File(DB_PATH)); // access the database from file system
			
			registerShutdownHook(graphDb);
		}
		return graphDb;
	}

	/**
	 * Closes the connection to the database.
	 */
	public static void closeGraphDb() {
		if (graphDb != null) {
			graphDb.shutdown();
		}
	}

	/**
	 * Drops the database by deleting all database files from the file system.
	 * 
	 * @throws IOException
	 *             If accessing the database path fails.
	 */
	public static void dropDatabase() throws IOException {
		closeGraphDb();
		FileUtils.deleteRecursively(new File(DB_PATH));
	}

	/**
	 * Registers a shutdown hook for the Neo4j instance:
	 * http://neo4j.com/docs/stable/tutorials-java-embedded-setup.html
	 * 
	 * @param graphDb
	 *            A graph database.
	 */
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

}
