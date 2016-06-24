package clustering;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import process_control.ClusterProcess;
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
	
	/** Graph statistics. */
	private final Statistics statistics;

	/**
	 * The minimum of cities a suffix node subsumes such that it represents a
	 * cluster.
	 */
	private int minClusterSize;
	
	/** Minimal cluster size wrt. to the tree (each last letter forms a tree). */
	private final float minPercent;

	/**
	 * The maximum of cities a suffix node subsumes such that it represents a
	 * cluster.
	 */
	private int maxClusterSize;
	
	/** Maximal cluster size wrt. to the tree (each last letter forms a tree). */
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
	 * @param statistics
	 *            Graph statistics.
	 */
	public SuffixClustering(GraphDatabaseService graphDb, GraphProperties properties, Statistics statistics) {
		this(graphDb, properties, statistics, 0f, 0f, 0f);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param graphDb
	 *            Graph database.
	 * @param properties
	 *            Graph properties.
	 * @param statistics
	 *            Graph statistics.
	 * 
	 * @param proportion
	 *            How many cities of the parent node must the current node
	 *            subsume in order to represent a cluster potentially.
	 */
	public SuffixClustering(GraphDatabaseService graphDb, GraphProperties properties, Statistics statistics, float proportion) {
		this(graphDb, properties, statistics, proportion, 0f, 0f);		
	}
	
	/**
	 * 
	 * @param graphDb
	 *            Graph database.
	 * @param properties
	 *            Graph properties.
	 * @param statistics
	 *            Graph statistics.
	 * @param proportion
	 *            How many cities of the parent node must the current node
	 *            subsume in order to represent a cluster potentially.
	 * @param minPercent
	 *             Minimal cluster size wrt. to the tree (each last letter forms a tree).
	 * @param maxPercent
	 *             Maximal cluster size wrt. to the tree (each last letter forms a tree).
	 */
	public SuffixClustering(GraphDatabaseService graphDb, GraphProperties properties, Statistics statistics, float proportion, float minPercent, float maxPercent) {
		if (minPercent < 0 || maxPercent < 0 || minPercent > 1 || maxPercent > 1)
			throw new IllegalArgumentException("You have to specify a value from (0,1) for percentMin/Max.");
		this.graphDb = graphDb;
		this.properties = properties;
		this.statistics = statistics;
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
				int noCities = ((Number) root.getUnderlyingNode().getProperty(Suffix.KEY_SUBSCITIES)).intValue();
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
					float expected = ((Number) parent.getProperty(Suffix.KEY_SUBSCITIES)).intValue() * this.proportion;
//					System.out.println(" p:" + parent.getProperty(Suffix.KEY_STR) + "\t subsCit:"
//							+ parent.getProperty(Suffix.KEY_SUBSCITIES) + "\t proportion:" + expected);

					// iterate through its children
					Iterator<Relationship> iterator = parent
							.getRelationships(Direction.OUTGOING, EdgeTypes.IS_SUFFIX_OF).iterator();
					while (iterator.hasNext()) {
						Node child = iterator.next().getEndNode();
						// check for cluster candidate
						int subsumedCities = ((Number) child.getProperty(Suffix.KEY_SUBSCITIES)).intValue();
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
	 * Returns the cluster candidates in a set.
	 * 
	 * @return Cluster candidates.
	 */
	public Set<Suffix> getClusterCandidates() {
		Set<Suffix> candidates = new HashSet<Suffix>();
		
		String cypher = "MATCH (n:"+Suffix.LABEL+") WHERE n."+Suffix.KEY_CLUSTER+" = true RETURN n";
		try(Transaction tx = this.graphDb.beginTx();
				Result result = this.graphDb.execute(cypher)) {
			
			while(result.hasNext()) {
				Map<String,Object> row = result.next();
		        for (Entry<String,Object> column : row.entrySet()) {
		            Suffix s = new Suffix((Node) column.getValue());
		            candidates.add(s);
		        }
			}
		}
		
		return candidates;
	}

	/**
	 * Iterates through the graph and determines suffix nodes which represent
	 * cluster candidates. Uses n-gram distribution as background knowledge.
	 * Cluster candidates are annotated in the DB with a property.
	 *  
	 * @throws NoSuchFieldException
	 *             If the property 'subsumed cities' is not part of the graph.
	 */
	public void determineClusterCandidatesByNGrams() throws NoSuchFieldException {
		// iterate over each root
		for (Suffix root : properties.getRootNodes()) {			
			// define min-/max cluster size for this tree
			try (Transaction tx = this.graphDb.beginTx()) {
				if (!root.getUnderlyingNode().hasProperty(Suffix.KEY_SUBSCITIES))
					throw new NoSuchFieldException("You need to determined subsumend cities for this method.");
				int noCities = ((Number) root.getUnderlyingNode().getProperty(Suffix.KEY_SUBSCITIES)).intValue();
				calculateMinMax(noCities);
			}			
			
			// initialize queue: contains parents of possible candidates
			Queue<Suffix> queue = new LinkedList<Suffix>();
			queue.add(root);
			// iterate through the tree
			while (queue.size() > 0) {
				// get first node of the queue
				Node parent = null;
				Map<String, Object> parentProperties = null;
				try (Transaction tx = this.graphDb.beginTx()) {
					parent = queue.remove().getUnderlyingNode();
					parentProperties = parent.getAllProperties();
				}
				
				// iterate through its children
				boolean isInheritance = parentProperties.containsKey(Suffix.KEY_CLUSTER);
				Set<Node> candidates = new HashSet<Node>();
				Iterator<Relationship> iterator = null;
				try (Transaction tx = this.graphDb.beginTx()) {
					iterator = parent.getRelationships(Direction.OUTGOING, EdgeTypes.IS_SUFFIX_OF).iterator();
				}
				while (iterator.hasNext()) {
					// get information of the current child
					Node child = null;
					int subsCitiesChild = -1;
					Map<String, Object> childProperties = null;
					try (Transaction tx = this.graphDb.beginTx()) {
						child = iterator.next().getEndNode();
						childProperties = child.getProperties(Suffix.KEY_STR, Suffix.KEY_SUBSCITIES);
						subsCitiesChild = ((Number) childProperties.get(Suffix.KEY_SUBSCITIES)).intValue();
					}
					
					// ##### decide whether child is cluster candidate #####
					if (subsCitiesChild >= this.minClusterSize) {
						// child or its children (!) could be a candidate
						queue.add(new Suffix(child));

						if (subsCitiesChild <= this.maxClusterSize) {
							// child has to be significant and long enough (|suffix| >= 3)
							boolean isSignificant = calculateSignificance(childProperties, parentProperties);
							
							isInheritance &= isSignificant;
							
							if (isSignificant)
								candidates.add(child);
						}
					} // #### 
					
				} // end iteration children
				
				// POST-PROCESSING
				// (1) a node is no candidate iff its parent and all its relevant sisters are candidates, too
				if (!isInheritance && candidates.size() > 0) {
					for (Node candidate : candidates) {
						try (Transaction tx = this.graphDb.beginTx()) {
							candidate.setProperty(Suffix.KEY_CLUSTER, true);
							tx.success();
						}
					}
					
					// (2) if the parent is a candidate (ensured by the following IF) 
					//		and at least one child is resp. is not a cluster candidate (ensured by the previous IF),
					//		then the parent is no cluster candidate, e.g. [_orf [ dorf ] [ torf ] ]
//					if (parentProperties.containsKey(Suffix.KEY_CLUSTER)) {
//						try (Transaction tx = this.graphDb.beginTx()) {
//							parent.removeProperty(Suffix.KEY_CLUSTER);
//							tx.success();
//						}
//					}	
					
				} // end post-processing
				
			} // end tree iteration				
		} // end iteration whole graph
		
		// global post-processing
//		int countMod = applyGlobalPostprocessing();
//		ClusterProcess.log.info("#(global mods): "+countMod);
		
	}
	
//	/**
//	 * Applies global constraints to the graph, i.e. post-processing constraints
//	 * which take into account the the whole graph.
//	 * 
//	 * @return Number of modifications of cluster candidate status.
//	 */
//	private int applyGlobalPostprocessing() {
//		int countMod = 0;
//		
//		// TODO: not sure about this query ... 
//		String cypher = "MATCH (p:"+Suffix.LABEL+")-[:"+EdgeTypes.IS_SUFFIX_OF+"]->(c:"+Suffix.LABEL+") "
//				+ "WHERE p."+Suffix.KEY_CLUSTER+"=true AND c."+Suffix.KEY_CLUSTER+"=true "
//						+ "SET p."+Suffix.KEY_CLUSTER+"=false " 
//								+ "RETURN count(p) AS count";
//		ClusterProcess.log.info("CYPHER: "+cypher);
//		
//		try(Transaction tx = this.graphDb.beginTx();
//				Result result = this.graphDb.execute(cypher)) {			
//			while (result.hasNext()) {
//				Map<String, Object> row = result.next();
//				countMod = (int) row.get("count");
//			}
//			tx.success();
//		}
//		
//		return countMod;		
//	}
	
	/**
	 * Calculates whether a suffix node is a cluster candidate, i.e. is significant.
	 * 
	 * @param childProperties
	 * 				Properties of the current suffix node (the child).
	 * @param parentProperties 
	 * 				Properties of the parent node of the current suffix node.
	 * 
	 * @return 'true' iff significant.
	 */
	private boolean calculateSignificance(Map<String, Object> childProperties, Map<String, Object> parentProperties) {
		boolean sign = false;
		
		// get values
		String strChild = (String) childProperties.get(Suffix.KEY_STR); // e.g.: "zell"
		char letter = strChild.charAt(0); // e.g.: "z"
		String bigram = (strChild.length() > 1) ? strChild.substring(0, 2) : ""+letter+statistics.eow; // e.g.: "ze"
		String trigram = (strChild.length() > 2) ? strChild.substring(0, 3) : bigram+statistics.eow; // e.g. "zel"
		char context1 = (strChild.length() > 1) ? strChild.charAt(1) : statistics.eow; // e.g.: "e"
		String context2 = (strChild.length() > 2) ? strChild.substring(1, 3) : ""+context1+statistics.eow; // e.g.: "el"
		
		Map<Character, Integer> letterDistribution = this.statistics.getLetterDistribution();
		Map<String, Integer> bigramDistribution = this.statistics.getBigramDistribution();
		Map<String, Integer> trigramDistribution = this.statistics.getTrigramDistribution();
		
		// calculate actual proportion
		float actual = 1f * ((Number) childProperties.get(Suffix.KEY_SUBSCITIES)).intValue()/((Number) parentProperties.get(Suffix.KEY_SUBSCITIES)).intValue();
				
		// considering no context, e.g. P(z)
		float p0 = 1f * letterDistribution.get(letter) / this.statistics.getNumberLetterTokens();
		
		// considering context of one letter, e.g. P(z|e)
		float pContext = 1f * letterDistribution.get(context1) / this.statistics.getNumberLetterTokens(); // e.g. P(e)
		float pBigram = 1f * bigramDistribution.get(bigram) / this.statistics.getNumberBigramTokens(); // e.g. P(ze)
		float p1 = pBigram / pContext;
		
		// considering context of two letters, e.g. P(z|el)
		pContext = 1f * bigramDistribution.get(context2) / this.statistics.getNumberBigramTokens(); // e.g. P(el)
		float pTrigram = 1f * trigramDistribution.get(trigram) / this.statistics.getNumberTrigramTokens();
		float p2 = pTrigram / pContext;
		
		// calculate significance
		float weight0 = 0.2f, weight1 = 0.3f, weight2 = 0.5f; // linear interpolation, sum(weights) := 1
		float p = weight0 * p0 + weight1 * p1 + weight2 * p2;
		
		float alpha = 1.5f;
		if (actual > p * alpha)
			sign = true;
		
		return sign;
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
		this.minClusterSize = Math.max(5, (int) (noCities * this.minPercent));
		this.maxClusterSize = Math.min(noCities - 2, (int) (noCities - noCities * this.maxPercent));
	}
	
	/**
	 * Removes the property 'clusterCandidate' from the graph.
	 */
	public void removeClusterCandidateProperty() {
		String cypher = "MATCH (n:"+Suffix.LABEL+") REMOVE n."+Suffix.KEY_CLUSTER;
		ClusterProcess.log.info("CYPHER: "+cypher);
		
		try(Transaction tx = this.graphDb.beginTx();
				Result result = this.graphDb.execute(cypher)) {
			
			//... 
			tx.success();
		}
	}
	
	
	
}
