package process_control;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import clustering.GraphProperties;
import clustering.Statistics;
import clustering.Visualisation;
import database.DatabaseAccess;
import etl.Extraction;
import etl.Load;
import representation.Suffix;

/**
 * Contains the main steps for clustering toponyms.
 * 
 * @author MM
 *
 */
public class ClusterProcess {

	/** Log4j Logger */
	public static Logger log = Logger.getLogger(ClusterProcess.class);

	/** Location of the raw data in the file system, e.g. worldcitiespop.txt */
	static final String locationRawData = "./src/main/resources/worldcitiespop.txt";
	/** Location of the extracted data in the file system. */
	static final String locationExtractedData = "./src/main/resources/extractedData.csv";
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
				DatabaseAccess.dropDatabase();
				graphDb = DatabaseAccess.getGraphDb();
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
		log.info("Determining graph properties ... ");
		GraphProperties properties = new GraphProperties(graphDb);
		long countCityNodes = properties.getCountCityNodes();
		long countSuffixNodes = properties.getCountSuffixNodes();
		int countLonelySuffixes = properties.getLonelySuffixes().size();
		int countNormalSuffixes = properties.getNormalSuffixes().size();
		int countFrequentSuffixes = properties.getFrequentSuffixes().size();
		int countVeryFrequentSuffixes = properties.getVeryFrequentSuffixes().size();
		int countRootNodes = properties.getRootNodes().size();
		
		log.info("Results:\n  countCityNodes:\t\t"+countCityNodes+
				"\n  countSuffixNodes:\t\t"+countSuffixNodes+
				"\n  countLonelySuffixes:\t\t"+countLonelySuffixes+
				"\n  countNormalSuffixes:\t\t"+countNormalSuffixes+
				"\n  countFreqSuffixes:\t\t"+countFrequentSuffixes+
				"\n  countVeryFreqSuffixes:\t"+countVeryFrequentSuffixes+
				"\n  countRootNodes:\t\t"+countRootNodes);
		
		try(Transaction tx = graphDb.beginTx()) {
			String logString = "Results:\n  roots: ";
			Set<String> roots = new TreeSet<String>();
			for (Suffix root : properties.getRootNodes())
				roots.add(root.getStr());
			for (String str : roots)
				logString += str+",";
			log.info(logString.substring(0, logString.length()-1));
			
		}
		
		// 3: add subsumed cities count to the database
		log.info("Adding property 'subsumedCities' ... ");
		properties.addPropertySubsumedCities();
		
		// 4: determine distribution of letters, bigrams, and trigrams
		log.info("Determining distribution of letters, bigrams, and trigrams ... ");
		Statistics statistics = new Statistics(properties);
		
		System.out.println("\nletter distribution:");
		Visualisation.printDistributionMap(statistics.getLetterDistribution(), statistics.getNumberLetterTokens());
		
		System.out.println("\nbigram distribution:");
		Visualisation.printDistributionMap(statistics.getBigramDistribution(), statistics.getNumberBigramTokens());
		
		System.out.println("\ntrigram distribution:");
		Visualisation.printDistributionMap(statistics.getTrigramDistribution(), statistics.getNumberTrigramTokens());
		
		// clean up
		log.info("Closing database ... ");
		DatabaseAccess.closeGraphDb();

		long timeEnd = System.currentTimeMillis();
		log.info("end");
		System.out.println("\n===== End (" + (timeEnd - timeStart) + "ms) =====");
	}

}
