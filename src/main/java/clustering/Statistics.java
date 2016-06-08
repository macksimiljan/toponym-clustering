package clustering;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import representation.Suffix;

/**
 * Statistics about n-grams of the city names.
 * 
 * @author MM
 *
 */
public class Statistics {
	
	/** Graph properties. */
	private GraphProperties properties;
	
	/** City name letters mapped to their count. */
	private Map<Character, Integer> letterDistribution;
	/** City name bigrams mapped to their count. */
	private Map<String, Integer> bigramDistribution;
	/** City name trigrams mapped to their count. */
	private Map<String, Integer> trigramDistribution;
	
	/** Number of letter tokens. */
	private int numberLetterTokens;
	/** Number of bigram tokens. */
	private int numberBigramTokens;
	/** Number of trigram tokens. */
	private int numberTrigramTokens;
		
	
	public Statistics (GraphProperties properties) {
		this.properties = properties;
		this.letterDistribution = new TreeMap<Character, Integer>();
		this.bigramDistribution = new TreeMap<String, Integer>();
		this.trigramDistribution = new TreeMap<String, Integer>();
		this.numberLetterTokens = 0;
		this.numberBigramTokens = 0;
		this.numberTrigramTokens = 0;
	}
	
	public int getNumberLetterTokens() {
		if (this.numberLetterTokens == 0)
			this.calculateNGrams();
		
		return this.numberLetterTokens;
	}
	
	public int getNumberLetterTypes() {
		if (this.letterDistribution.size() == 0)
			this.calculateNGrams();
		
		return this.letterDistribution.size();
	}
	
	public int getNumberBigramTokens() {
		if (this.numberBigramTokens == 0)
			this.calculateNGrams();
		
		return this.numberBigramTokens;
	}
	
	public int getNumberBigramTypes() {
		if (this.bigramDistribution.size() == 0)
			this.calculateNGrams();
		
		return this.bigramDistribution.size();
	}
	
	public int getNumberTrigramTokens() {
		if (this.numberTrigramTokens == 0)
			this.calculateNGrams();
		
		return this.numberTrigramTokens;
	}
	
	public int getNumberTrigramTypes() {
		if (this.trigramDistribution.size() == 0)
			this.calculateNGrams();
		
		return this.trigramDistribution.size();
	}	
	
	public float getNumberBigramForUniformDistrib() {
		return this.getNumberBigramTokens()/1f*this.getNumberBigramTypes();
	}
	
	public float getNumberTrigramForUniformDistrib() {
		return this.getNumberTrigramTokens()/1f*this.getNumberTrigramTypes();
	}
	
	
	
	/**
	 * Returns the distribution of letters within the city names.
	 * @return City name letters mapped to their count. 
	 */
	public Map<Character, Integer> getLetterDistribution() {
		if (this.letterDistribution.size() == 0)
			this.calculateNGrams();
		
		return this.letterDistribution;		
	}
	
	/**
	 * Returns the distribution of bigrams within the city names.
	 * @return City name bigrams mapped to their count. 
	 */
	public Map<String, Integer> getBigramDistribution() {
		if (this.bigramDistribution.size() == 0)
			this.calculateNGrams();
		
		return this.bigramDistribution;
	}
	
	/**
	 * Returns the distribution of trigrams within the city names.
	 * @return City name trigrams mapped to their count. 
	 */
	public Map<String, Integer> getTrigramDistribution() {
		if (this.trigramDistribution.size() == 0)
			this.calculateNGrams();
		
		return this.trigramDistribution;
	}
	
	public Map<Character, Integer> sortLetterDistributionByCount() {
		return this.sortDistribByCount(this.letterDistribution);
	}
	
	public Map<String, Integer> sortBigramDistributionByCount() {
		return this.sortDistribByCount(this.bigramDistribution);
	}
	
	public Map<String, Integer> sortTrigramDistributionByCount() {
		return this.sortDistribByCount(this.trigramDistribution);
	}
	
	private <K extends Comparable<? super K>, V extends Comparable<? super V>> Map<K,V> sortDistribByCount(Map<K,V> distrib) {
		Map<K, V> result = new LinkedHashMap<>();
		
		List<Map.Entry<K, V>> list = new LinkedList<>(distrib.entrySet());
		
		Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
			@Override
		    public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 ) {
				int z = -1 * o1.getValue().compareTo(o2.getValue());
				if (z == 0)
					z = o1.getKey().compareTo(o2.getKey());
					
				return z;
			}
		} );

		for (Map.Entry<K, V> entry : list) {
			result.put( entry.getKey(), entry.getValue() );
		}
		
		return result;
	}
	
	/**
	 * Calculates the n-grams (n=1, n=2, n=3) for all city names.
	 */
	private void calculateNGrams() {
		final char sow = '$'; // start of word
		final char eow = '#'; // end of word
		
		// determine n grams
		Set<Suffix> cityNames = properties.getCityNames();
		for (Suffix cityName : cityNames) {
			String str = cityName.getStr();
			char[] letters = new char[str.length()+4];
			letters[0] = sow;
			letters[1] = sow;
			letters[letters.length-2] = eow;
			letters[letters.length-1] = eow;			
			str.getChars(0, str.length(), letters, 2);
			
			// iterate over the letters of a city name
			for (int i=2; i<letters.length; i++) {
				// 1: letter distribution (eow's are ignored)
				if (i < letters.length - 2) {
					Integer oldValueLetter = this.letterDistribution.get(letters[i]);
					if (oldValueLetter == null)					
						this.letterDistribution.put(letters[i], 1); // add the the new letter to the distribution
					else					
						this.letterDistribution.put(letters[i], oldValueLetter + 1); // increment counter
				}				
				
				// 2: bigram distribution ([eow, eow] is ignored)
				if (i < letters.length - 1) {
					char[] bigramArray = {letters[i-1], letters[i]};
					String bigram = String.copyValueOf(bigramArray);
					Integer oldValueBigram = this.bigramDistribution.get(bigram);
					if (oldValueBigram == null)
						this.bigramDistribution.put(bigram, 1); // add the new bigram to the distribution
					else
						this.bigramDistribution.put(bigram, oldValueBigram + 1); // increment counter
				}				
				
				// 3: trigram distribution
				char[] trigramArray = {letters[i-2], letters[i-1], letters[i]};
				String trigram = String.copyValueOf(trigramArray);
				Integer oldValueTrigram = this.trigramDistribution.get(trigram);
				if (oldValueTrigram == null)
					this.trigramDistribution.put(trigram, 1); // add the new trgram to the distribution
				else
					this.trigramDistribution.put(trigram, oldValueTrigram + 1); // increment counter
				
			} // end iteration over letters
			
			this.numberLetterTokens += str.length();
			this.numberBigramTokens += str.length() + 1;
			this.numberTrigramTokens += str.length() + 2;
		} // end iteration over city names
	}
	

}
