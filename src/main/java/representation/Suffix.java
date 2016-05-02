package representation;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * Domain entity of a suffix node.
 * 
 * @author MM
 *
 */
public class Suffix {
	
	/** Label of the domain entity. */
	public static final Label LABEL = DynamicLabel.label("suffix");

	/** Property label 'str'. */
	public static final String STR = "str";
	/** The underlying node of this city. */
	private final Node underlyingNode;

	/**
	 * Creates a new suffix entity.
	 * 
	 * @param underlyingNode
	 *            The underlying node of the new suffix entity.
	 */
	public Suffix(Node underlyingNode) {
		this.underlyingNode = underlyingNode;
	}

	/**
	 * Returns the node representation of this suffix within the database.
	 * 
	 * @return The underlying node.
	 */
	protected Node getUnderlyingNode() {
		return underlyingNode;
	}

	/**
	 * Returns the string value of this suffix.
	 * 
	 * @return A string representing the suffix.
	 */
	public String getStr() {
		return (String) this.underlyingNode.getProperty(STR);
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
		return "Suffix: " + getStr();
	}

}
