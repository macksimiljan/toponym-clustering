package representation;

import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import database.DatabaseAccess;

/**
 * Domain entity of a geo-statistics node.
 * 
 * @author MM
 *
 */
public class GeoStatistics {
	
	/** Label of the domain entity. */
	public static final Label LABEL = DynamicLabel.label("geoStatistics");
	
	/** Property label 'min' (minimum). */
	public static final String KEY_MIN = "min";
	
	/** Property label 'max' (maximum).*/
	public static final String KEY_MAX = "max";
	
	/** Property label 'avg' (average, mean). */
	public static final String KEY_AVG = "avg";
	
	/** Property label 'dev' (standard deviation). */
	public static final String KEY_DEV = "dev";
	
	/** The underlying node of this geo-statistics. */
	private final Node underlyingNode;
	
	private double min;
	private double max;
	private double avg;
	private double dev;
	
	/**
	 * Creates a new geo-statistics entity.
	 * 
	 * @param underlyingNode
	 * 				The underlying node of the new geo-statistics entity.
	 */
	public GeoStatistics(Node underlyingNode) {
		this.underlyingNode = underlyingNode;
		min = -1; max = -1; avg = -1; dev = -1;
	}	
	
	/**
	 * Returns the node representation of this geo-statistics within the database.
	 * 
	 * @return The underlying node.
	 */
	public Node getUnderlyingNode() {
		return this.underlyingNode;
	}
	
	public double getMin() {
		if (min == -1)
			setProperties();
		
		return min;		
	}
	
	public double getMax() {
		if (max == -1)
			setProperties();
		
		return max;
	}
	
	public double getAvg() {
		if (avg == -1)
			setProperties();
		
		return avg;
	}
	
	public double getDev() {
		if (dev == -1)
			setProperties();
		
		return dev;
	}
	
	
	private void setProperties() {
		try (Transaction tx = DatabaseAccess.getGraphDb().beginTx()) {
			Map<String, Object> properties = underlyingNode.getAllProperties();
			min = (double) properties.get(KEY_MIN);
			max = (double) properties.get(KEY_MAX);
			avg = (double) properties.get(KEY_AVG);
			dev = (double) properties.get(KEY_DEV);
		}
	}
	
	@Override
	public int hashCode() {
		return underlyingNode.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof Suffix && underlyingNode.equals(((Suffix) o).getUnderlyingNode());
	}

	public String toString() {
		return "GeoStatistics: min="+getMin()
				+ "\t max="+getMax()
				+ "\t avg="+getAvg()
				+ "\t dev="+getDev();
	}

}
