package etl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import representation.City;
import representation.EdgeTypes;
import representation.Suffix;

/**
 * Loads the city data into the graph database.
 * 
 * @author MM
 *
 */
public class Load {

	/**
	 * Loads city and suffix nodes as well as the relationships among them to
	 * the graph database.
	 * 
	 * @param graphDb
	 *            The graph database.
	 * @param data
	 *            List of maps containing information to cities. Allowed keys
	 *            are 'city' (= name of the city), 'latitude', and 'longitude'.
	 */
	public static void loadCityAndSuffix(GraphDatabaseService graphDb, List<Map<String, String>> data) {
		// create unique constraint for attribute 'str' in suffix-nodes
		try (Transaction tx = graphDb.beginTx()) {
			graphDb.schema().constraintFor(Suffix.LABEL).assertPropertyIsUnique(Suffix.STR).create();
			tx.success();
		}

		// iterate over each row of the data
		for (Map<String, String> row : data) {
			if (Integer.parseInt(row.get("id")) % 1000 == 0)
				System.out.println("\t"+row.get("id"));
			String cityName = row.get("city");
			float latitude = Float.parseFloat(row.get("latitude"));
			float longitude = Float.parseFloat(row.get("longitude"));

			try (Transaction tx = graphDb.beginTx()) {
				// insert city node
				Node newCity = graphDb.createNode();
				newCity.addLabel(City.LABEL);
				newCity.setProperty(City.LATITUDE, latitude);
				newCity.setProperty(City.LONGITUDE, longitude);

				// insert suffix nodes: use MERGE because suffix is unique
				String mergeQuery = "MERGE (n:" + Suffix.LABEL + " {" + Suffix.STR + ": {value}})";
				String edgeQuery = "MATCH (source:" + Suffix.LABEL + " {" + Suffix.STR + ": {valSrc}}), (target:"
						+ Suffix.LABEL + " {" + Suffix.STR + ": {valTrg}}) CREATE UNIQUE (source)-[:"
						+ EdgeTypes.IS_SUFFIX_OF + "]->(target)";
				Map<String, Object> parameters = new HashMap<>();
				for (int i = cityName.length(); i > 0; i--) {
					String str = cityName.substring(cityName.length() - i);
					parameters.put("value", str);
					graphDb.execute(mergeQuery, parameters);

					// insert edge
					Node source = graphDb.findNode(Suffix.LABEL, Suffix.STR, str);
					if (i == cityName.length()) {
						// isNameOf edge
						source.createRelationshipTo(newCity, EdgeTypes.IS_NAME_OF);
					} else {
						// isSuffixOf edge
						String prevSuffix = cityName.substring(cityName.length() - i - 1);
						
						parameters.put("valSrc", str);
						parameters.put("valTrg", prevSuffix);
						graphDb.execute(edgeQuery, parameters);
					}

				}

				tx.success();
			}
		}
	}

}
