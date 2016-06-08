package clustering;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import representation.EdgeTypes;
import representation.Suffix;

/**
 * Provides methods for clustering city names according to their 'ending'. You
 * can instantiate this class with different critical values and thus compare
 * the results within one workflow.
 * 
 * @author MM
 *
 */
public class SuffixClustering {

	/** Graph database. */
	private final GraphDatabaseService graphDb;

	/** Graph properties. */
	private final GraphProperties properties;

	/**
	 * The minimum of cities a suffix node subsumes such that it represents a
	 * cluster.
	 */
	private int minClusterSize;
	
	private final float minPercent;

	/**
	 * The maximum of cities a suffix node subsumes such that it represents a
	 * cluster.
	 */
	private int maxClusterSize;
	
	private final float maxPercent;

	/**
	 * How many cities (proportion) of the parent node must the current node
	 * subsume in order to represent a cluster potentially.
	 */
	private float proportion;

	/**
	 * @return the minClusterSize
	 */
	public int getMinClusterSize() {
		return minClusterSize;
	}

	/**
	 * @return the maxClusterSize
	 */
	public int getMaxClusterSize() {
		return maxClusterSize;
	}

	/**
	 * Constructor.
	 * 
	 * @param graphDb
	 *            Graph database.
	 * @param properties
	 *            Graph properties.
	 * 
	 * @param proportion
	 *            How many cities of the parent node must the current node
	 *            subsume in order to represent a cluster potentially.
	 */
	public SuffixClustering(GraphDatabaseService graphDb, GraphProperties properties, float proportion) {
		this(graphDb, properties, proportion, 0f, 0f);
		
	}
	
	public SuffixClustering(GraphDatabaseService graphDb, GraphProperties properties, float proportion, float minPercent, float maxPercent) {
		if (minPercent < 0 || maxPercent < 0 || minPercent > 1 || maxPercent > 1)
			throw new IllegalArgumentException("You have to specify a value from (0,1) for percentMin/Max.");
		this.graphDb = graphDb;
		this.properties = properties;
		this.proportion = proportion;
		this.minPercent = minPercent;
		this.maxPercent = maxPercent;
	}

	/**
	 * Iterates through the graph and determines suffix nodes which represent
	 * cluster candidates.
	 * 
	 * @return Cluster candidates for suffixes.
	 * @throws NoSuchFieldException
	 *             If the property 'subsumed cities' is not part of the graph.
	 */
	public Set<Suffix> determineClusterCandidatesByProportion() throws NoSuchFieldException {
		Set<Suffix> candidates = new HashSet<Suffix>();

		// iterate over each root
		for (Suffix root : properties.getRootNodes()) {
			try (Transaction tx = this.graphDb.beginTx()) {
				// define min-/max cluster size for this tree
				if (!root.getUnderlyingNode().hasProperty(Suffix.KEY_SUBSCITIES))
					throw new NoSuchFieldException("You need to determined subsumend cities for this method.");
				int noCities = (Integer) root.getUnderlyingNode().getProperty(Suffix.KEY_SUBSCITIES);
				calculateMinMax(noCities);

				// initialize queue
				Queue<Suffix> queue = new LinkedList<Suffix>();
				queue.add(root);

				// iterate through the tree
				while (queue.size() > 0) {

//					System.out.println();
//					System.out.println(queue.toString());

					// get first node of the queue
					Node parent = queue.remove().getUnderlyingNode();
					float expected = ((int) parent.getProperty(Suffix.KEY_SUBSCITIES)) * this.proportion;
//					System.out.println(" p:" + parent.getProperty(Suffix.KEY_STR) + "\t subsCit:"
//							+ parent.getProperty(Suffix.KEY_SUBSCITIES) + "\t proportion:" + expected);

					// iterate through its children
					Iterator<Relationship> iterator = parent
							.getRelationships(Direction.OUTGOING, EdgeTypes.IS_SUFFIX_OF).iterator();
					while (iterator.hasNext()) {
						Node child = iterator.next().getEndNode();
						// check for cluster candidate
						int subsumedCities = (int) child.getProperty(Suffix.KEY_SUBSCITIES);
//						System.out.println(" c:" + child.getProperty(Suffix.KEY_STR) + "\t subsCit:" + subsumedCities);

						if (subsumedCities >= this.minClusterSize) {
							// child or its children (!) could be a candidate
							queue.add(new Suffix(child));
							// calculate proportion
							if (subsumedCities <= this.maxClusterSize && subsumedCities >= expected) {
								// expected size and proportion
//								System.out.println(" --> cluster candidate");
								candidates.add(new Suffix(child));
								// if parent is cluster candidate: remove parent
								candidates.remove(new Suffix(parent));
//								System.out.println("\t"+candidates.contains(new Suffix(parent)));
//								System.out.println("\t"+candidates);
							}
						}
					} // end children iteration
				} // end tree iteration
			}
		} // end iteration whole graph

		return candidates;
	}

	

	/**
	 * Calculates the min and max size measure for a cluster candidate. Min and
	 * max depends on the user specified proportions w.r.t. the number of
	 * cities.
	 * 
	 * @param noCities
	 *            Number of cities within a connected component.
	 * @throws IllegalArgumentException
	 *             If proportions are not correct.
	 */
	public void calculateMinMax(int noCities) throws IllegalArgumentException {
		this.minClusterSize = Math.max(2, (int) (noCities * this.minPercent));
		this.maxClusterSize = Math.min(noCities - 1, (int) (noCities - noCities * this.maxPercent));
	}

	
}
