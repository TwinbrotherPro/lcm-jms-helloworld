package twinbrother.de.jms.fruitapp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public enum FruitType {
	GRAPEFRUIT, APPLE, STRAWBERRY, PEAR, RASPBERRY, LINGONBERRY, LEMON, BANANA; 
	
	 private static final List<FruitType> VALUES =
	    Collections.unmodifiableList(Arrays.asList(values()));
	 private static final int SIZE = VALUES.size();
	 private static final Random RANDOM = new Random();
	 
	 public static FruitType randomFruitType()  {
		    return VALUES.get(RANDOM.nextInt(SIZE));
	 }
}