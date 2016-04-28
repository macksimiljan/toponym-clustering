package representation;

import org.neo4j.graphdb.RelationshipType;

/**
 * 
 * Manages the edge types of the graph. Valid edge types are i) 'isNameOf',
 * which connects a suffix node with a city node whereby the suffix is equal to
 * the full name of the city; and ii) 'isSuffixOf', which connects to suffix
 * nodes.
 * 
 * @author MM
 *
 */
public enum EdgeTypes implements RelationshipType {

	/** (:suffix)-->(:city) */
	IS_NAME_OF,
	/** (:suffix)-->(:suffix) */
	IS_SUFFIX_OF;
}
