package clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import representation.City;
import representation.EdgeTypes;
import representation.GeoStatistics;
import representation.Suffix;

/**
 * Distance measure due to longitude and latitude of the cities.
 * 
 * @author MM
 *
 */
public class GeoDistance {

	/** Graph database. */
	private GraphDatabaseService graphDb;
	
	/** Geo-statistics of the current suffix node. */
	private GeoStatistics currGeoStat = null;

	/**
	 * Constructor. Creates a new instance of geo distance measuring.
	 * 
	 * @param graphDb
	 *            A graph database.
	 */
	public GeoDistance(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}
	
	/**
	 * Returns the geo-statistics of the current suffix node.
	 * 
	 * @return Geo-statistics (min, max, avg, dev, ... ). 
	 */
	public GeoStatistics getCurrGeoStatistics() {
		return this.currGeoStat;
	}
	

	/**
	 * Calculates the average Euclidean distance between the cities of one
	 * suffix and cluster candidate, respectively.
	 * 
	 * @param s
	 *            A suffix node, represents a cluster.
	 */
	public void calcAvgEuclideanDist(Suffix s) {
		// 0: check whether euclidean distance statistics is already calculated
		try (Transaction tx = this.graphDb.beginTx()) {
			boolean hasEuclideanDist = s.getUnderlyingNode().hasRelationship(EdgeTypes.EUCLIDEAN_DIST);
			if (hasEuclideanDist) {
				Node node = s.getUnderlyingNode().getRelationships(EdgeTypes.EUCLIDEAN_DIST)
						.iterator().next().getOtherNode(s.getUnderlyingNode());
				this.currGeoStat = new GeoStatistics(node);
				return;
			}
		}		
		
		
		// 1: calculate Euclidean distance statistics
		final Set<City> cities = s.getAssocCityLocations(this.graphDb);
		final List<City> cityList = new ArrayList<City>(cities);
		EuclideanDistance euclDist = new EuclideanDistance();
		
		DescriptiveStatistics stat = new DescriptiveStatistics();
		
		// get location of each city associated with the given suffix
		for (int i = 0; i < cityList.size() - 1; i++)
			for (int j = i+1; j < cityList.size(); j++) {
				City city1 = cityList.get(i);
				City city2 = cityList.get(j);
				double lat1 = ((Number) city1.getLatitude()).doubleValue();
				double lon1 = ((Number) city1.getLongitude()).doubleValue();
				double lat2 = ((Number) city2.getLatitude()).doubleValue();
				double lon2 = ((Number) city2.getLongitude()).doubleValue();
				double[] location1 = {lat1, lon1};
				double[] location2 = {lat2, lon2};
				
				double d = euclDist.compute(location1, location2);
				stat.addValue(d);
			}		
		
		double avg = stat.getMean();
		double max = stat.getMax();
		double min = stat.getMin();
		double dev = stat.getStandardDeviation();
		
		// write result to database
		try (Transaction tx = this.graphDb.beginTx()) {
			// create new statistics node and connect it to the given suffix
			Node node = this.graphDb.createNode(GeoStatistics.LABEL);
			node.createRelationshipTo(s.getUnderlyingNode(), EdgeTypes.EUCLIDEAN_DIST);
			// add properties
			node.setProperty(GeoStatistics.KEY_MIN, min);
			node.setProperty(GeoStatistics.KEY_MAX, max);
			node.setProperty(GeoStatistics.KEY_AVG, avg);
			node.setProperty(GeoStatistics.KEY_DEV, dev);
			
			this.currGeoStat = new GeoStatistics(node);
			
			tx.success();
		}		
	}

}
