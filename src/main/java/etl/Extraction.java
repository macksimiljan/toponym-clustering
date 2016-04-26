package etl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Extracts data from one or more data sources. Such data sources contains
 * information about German cities: name, latitude, and longitude.
 * 
 * @author MM
 *
 */
/**
 * @author MM
 *
 */
public abstract class Extraction {

	/**
	 * Extracts German city name (ASCII), latitude, and longitude from the Free
	 * World City Database.
	 * https://www.maxmind.com/de/free-world-cities-database
	 * 
	 * @param fileLocation
	 *            Location of the database as CSV file at the file system.
	 * 
	 * @return Extracted data for the attributes 'name', 'latitude', and
	 *         'longitude'.
	 * @throws IOException
	 *             If reading the CSV file from the file location fails.
	 */
	public static List<Map<String, String>> extractFromFreeWorldCitiesDatabase(String fileLocation) throws IOException {
		List<Map<String, String>> data = new ArrayList<Map<String, String>>();

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileLocation));
			// iterate over each row of the CSV file
			String row;
			while ((row = reader.readLine()) != null) {
				String[] rowData = row.split(",");
				if (rowData.length != 7) {
					throw new IOException("There is something wrong: 7 columns expected but there are " + rowData.length
							+ " columns in line \"" + row + "\".");
				}
				// check whether it is a German city
				if (rowData[0].equals("de")) {
					Map<String, String> extractedMap = new HashMap<String, String>();
					extractedMap.put("city", rowData[1]);
					extractedMap.put("latitude", rowData[5]);
					extractedMap.put("longitude", rowData[6]);
					data.add(extractedMap);
				}
			}
		} finally {
			if (reader != null)
				reader.close();
		}

		return data;
	}

	/**
	 * Writes the extracted data to the file system.
	 * 
	 * @param fileLocation
	 *            Location within the file system for the extracted data.
	 * @param data
	 *            The extracted data.
	 * @throws IOException
	 *             If writing to the file system fails.
	 */
	public static void writeToCsvFile(String fileLocation, List<Map<String, String>> data) throws IOException {
		int id = 1;
		ICsvMapWriter mapWriter = null;
		try {
			// prepare writer
			final CsvPreference PREF = CsvPreference.STANDARD_PREFERENCE;
			final CellProcessor[] PROC = new CellProcessor[] {
					new Optional(), // id
					new Optional(), // city
					new Optional(), // latitude
					new Optional() // longitude
			};
			final String[] HEADER = new String[] { "id", "city", "latitude", "longitude" };			
			mapWriter = new CsvMapWriter(new FileWriter(fileLocation), PREF);
			// write header and rows
			mapWriter.writeHeader(HEADER);
			for (Map<String, String> map : data) {
				map.put("id", Integer.toString(id++));
				mapWriter.write(map, HEADER, PROC);
			}
		} finally {
			if (mapWriter != null) {
				mapWriter.close();
			}
		}
	}

}
