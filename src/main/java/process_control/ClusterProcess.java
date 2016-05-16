package process_control;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;

import clustering.GraphProperties;
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
	static final String locationRawData = "./src/main/resources/worldcitiespop_small.txt";
	/** Location of the extracted data in the file system. */
	static final String locationExtractedData = "./src/main/resources/extractedData_small.csv";
	/** 'true' iff the graph database is already loaded. */
	static final boolean isGraphLoaded = true;

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

		GraphDatabaseService graphDb = DatabaseAccess.getGraphDb();
		
		// 1: ETL
		if (!isGraphLoaded) {
			try {
//				DatabaseAccess.dropDatabase();
				// extraction
				List<Map<String, String>> data = Extraction.extractFromFreeWorldCitiesDatabase(locationRawData);
				log.info("Writing to " + locationExtractedData + " ... ");
				Extraction.writeToCsvFile(locationExtractedData, data);
				// load
				log.info("Loading data to neo4j ... ");
				Load.loadCityAndSuffix(graphDb, data);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// 2: some properties of the graph
		GraphProperties properties = new GraphProperties(graphDb);
		long countCityNodes = properties.getCountCityNodes();
		long countSuffixNodes = properties.getCountSuffixNodes();
		int countLonelySuffixes = properties.getLonelySuffixes().size();
		int countNormalSuffixes = properties.getNormalSuffixes().size();
		int countFrequentSuffixes = properties.getFrequentSuffixes().size();
		int countVeryFrequentSuffixes = properties.getVeryFrequentSuffixes().size();
		
		System.out.println("countCityNodes:\t\t"+countCityNodes+
				"\ncountSuffixNodes:\t"+countSuffixNodes+
				"\ncountLonelySuffixes:\t"+countLonelySuffixes+
				"\ncountNormalSuffixes:\t"+countNormalSuffixes+
				"\ncountFrequentSuffixes:\t"+countFrequentSuffixes+
				"\ncountVeryFreqSuffixes:\t"+countVeryFrequentSuffixes);

		// clean up
		DatabaseAccess.closeGraphDb();

		long timeEnd = System.currentTimeMillis();
		log.info("end");
		System.out.println("\n===== End (" + (timeEnd - timeStart) + "ms) =====");
	}

}
