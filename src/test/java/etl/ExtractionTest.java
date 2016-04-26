package etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * Tests for {@link Extraction}.
 * 
 * @author MM
 *
 */
public class ExtractionTest {

	/**
	 * Test method for
	 * {@link Extraction#extractFromFreeWorldCitiesDatabase(String)}.
	 */
	@Test
	public void testExtractFromFreeWorldCitiesDatabase() {
		final String FILE_LOC = "./src/main/resources/worldcitiespop.txt";
		try {
			List<Map<String, String>> data = Extraction.extractFromFreeWorldCitiesDatabase(FILE_LOC);
			assertEquals(70477, data.size());
		} catch (IOException e) {
			e.printStackTrace();
			fail("Something went wrong.");
		}

	}

}
