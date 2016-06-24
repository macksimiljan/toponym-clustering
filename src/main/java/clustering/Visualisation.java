package clustering;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
			
			String valueStr = String.valueOf(value);
			while ((valueStr.length() - 10) < 0) {
				valueStr = " "+valueStr;
			}
			
			String delimiter = (normValue < 10) ? " " : "";
			System.out.println(key + ": " + valueStr + "\t(" + delimiter + normValue + "%)  " + bar);
		}
	}
	
	/**
	 * Exports a simple bar diagram to the file system. The length of the bar ranges
	 * from 1 to 100 and represents the percentage of the counts.
	 * @param path
	 * @param map
	 * @param base
	 * @throws IOException
	 */
	public static <T> void exportDistributionMap(String path, Map<T, Integer> map, int base) throws IOException {
		
		try (PrintWriter writer = new PrintWriter (new BufferedWriter (new FileWriter(path)));) {
			for (T key : map.keySet()) {
				int value = map.get(key);
				float normValue = value * 100.0f / base;
				String bar = "=";
				for (int i = 1; i < normValue; i++)
					bar += "=";
				
				String valueStr = String.valueOf(value);
				while ((valueStr.length() - 10) < 0) {
					valueStr = " "+valueStr;
				}
				
				writer.println(key + "\t" + valueStr + "\t" + normValue + "\t" + bar);
			}
		}		
	}
	
	//TODO: logarithmisch skalieren
	
}
