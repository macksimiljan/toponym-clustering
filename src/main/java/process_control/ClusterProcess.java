package process_control;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;

import database.DatabaseAccess;
import etl.Extraction;
import etl.Load;

/**
 * Contains the main steps for clustering toponyms.
 * 
 * @author MM
 *
 */
public class ClusterProcess {

	/** Log4j Logger */
	static Logger log = Logger.getLogger(ClusterProcess.class);

	/** Location of the raw data in the file system, e.g. worldcitiespop.txt */
	static final String locationRawData = "./src/main/resources/worldcitiespop.txt";
	/** Location of the extracted data in the file system. */
	static final String locationExtractedData = "./src/main/resources/extractedData.csv";

	/**
	 * Main steps for clustering toponyms.
	 * 
	 * @param args
	 *            console arguments, not used yet
	 */
	public static void main(String[] args) {
		System.out.println("===== Toponym Clustering =====\n");
		log.info("start");
		long timeStart = System.currentTimeMillis();
		
		try {
			DatabaseAccess.dropDatabase();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		GraphDatabaseService graphDb = DatabaseAccess.getGraphDb();
		try {
			List<Map<String, String>> data = Extraction.extractFromFreeWorldCitiesDatabase(locationRawData);
			log.info("Writing to " + locationExtractedData + " ... ");
			Extraction.writeToCsvFile(locationExtractedData, data);
			log.info("Loading data to neo4j ... ");
			Load.loadCityAndSuffix(graphDb, data);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// clean up
		DatabaseAccess.closeGraphDb();
		
		long timeEnd = System.currentTimeMillis();
		log.info("end");
		System.out.println("\n===== End (" + (timeEnd - timeStart) + "ms) =====");
	}

}
