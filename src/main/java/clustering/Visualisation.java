package clustering;

import java.io.IOException;
import java.util.Map;

/**
 * Contains visualization methods.
 * 
 * @author MM
 *
 */
public class Visualisation {

	/**
	 * Prints a simple bar diagram to the console.
	 * 
	 * @param map
	 *            A map to counts.
	 * @throws IOException
	 *             If the counts are too big.
	 */
	public static <T> void printDistributionMap(Map<T, Integer> map) throws IOException {
		for (T key : map.keySet()) {
			int value = map.get(key);
			if (value > 500)
				throw new IOException("Your data is too big.");
			String bar = "=";
			for (int i = 1; i < value; i++)
				bar += "=";
			String delimiter = (value < 10) ? "   " : ((value < 100) ? "  " : " ");
			System.out.println(key + " (" + value + "):" + delimiter + bar);
		}
	}

	/**
	 * Prints a simple bar diagram to the console. The length of the bar ranges
	 * from 1 to 100 and represents the percentage of the counts.
	 * 
	 * @param map
	 *            A map to counts.
	 * @param base
	 *            Sum of all counts.
	 */
	public static <T> void printDistributionMap(Map<T, Integer> map, int base) {
		for (T key : map.keySet()) {
			int value = map.get(key);
			int normValue = Math.round(value * 100.0f / base);
			String bar = "=";
			for (int i = 1; i < normValue; i++)
				bar += "=";
			String delimiter = (normValue < 10) ? "  " : " ";
			System.out.println(key + ": " + value + "\t(" + normValue + "%) " + delimiter + bar);
		}
	}

}
