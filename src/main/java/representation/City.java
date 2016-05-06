package representation;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * 
 * Domain entity for a city node.
 * 
 * @author MM
 *
 */
public class City {

	/** Label of the domain entity. */
	public static final Label LABEL = DynamicLabel.label("city");

	/** Property label 'latitude'. */
	public static final String LATITUDE = "latitude";
	/** Property label 'longitude'. */
	public static final String LONGITUDE = "longitude";

	/** The underlying node of this city. */
	private final Node underlyingNode;

	/**
	 * Creates a new city entity.
	 * 
	 * @param underlyingNode
	 *            The underlying node of the new city entity.
	 */
	public City(Node underlyingNode) {
		this.underlyingNode = underlyingNode;
	}

	/**
	 * Returns the node representation of this city within the database.
	 * 
	 * @return The underlying node.
	 */
	protected Node getUnderlyingNode() {
		return underlyingNode;
	}

	/**
	 * Returns the latitude value of this city.
	 * 
	 * @return A float number representing the latitude.
	 */
	public float getLatitude() {
		return (Float) this.underlyingNode.getProperty(LATITUDE);
	}

	/**
	 * Returns the longitude value of this city.
	 * 
	 * @return A float number representing the longitude.
	 */
	public float getLongitude() {
		return (Float) this.underlyingNode.getProperty(LONGITUDE);
	}

	/**
	 * Returns the name of this city. If no suffix node is connected to this
	 * city, then <code>null</code> is returned. If there is more than one name,
	 * then the different names are concatenated by using ','.
	 * 
	 * @return The name of this city.
	 */
	public String getNameOfTheCity() {
		int numberOfNames = this.underlyingNode.getDegree(EdgeTypes.IS_NAME_OF);

		if (numberOfNames == 0) {
			return null;
		}
		if (numberOfNames == 1) {
			Relationship edge = this.underlyingNode.getSingleRelationship(EdgeTypes.IS_NAME_OF, Direction.INCOMING);
			return (String) edge.getStartNode().getProperty(Suffix.KEY_STR);
		} else {
			String name = "";
			Iterable<Relationship> edges = this.underlyingNode.getRelationships(Direction.INCOMING,
					EdgeTypes.IS_NAME_OF);
			for (Relationship edge : edges) {
				name += edge.getStartNode().getProperty(Suffix.KEY_STR) + ",";
			}
			return name.substring(0, name.length() - 1);
		}
	}

	@Override
	public int hashCode() {
		return underlyingNode.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof City && underlyingNode.equals(((City) o).getUnderlyingNode());
	}

	public String toString() {
		return "City: (" + getLatitude() + "," + getLongitude() + ")";
	}

}
