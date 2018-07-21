import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;

import java.util.Set;
import java.util.TreeMap;

public class Sequential {

	public static void main(String[] args) throws IOException {
		FileInputStream fis = new FileInputStream(new File("ratings.dat"));
		
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		Map<Integer, Double> movieRating = new TreeMap<Integer, Double>();
		Map<Integer, Integer> movieNumRating = new TreeMap<Integer, Integer>();
		
		while((line = br.readLine()) != null) {
	        String[] tokens = line.split("::");
	        int movieID = Integer.parseInt(tokens[1]);
	        double rating = Double.parseDouble(tokens[2]);
	        
	        if(movieRating.containsKey(movieID)){
	        	movieRating.put(movieID, rating+movieRating.get(movieID));
	        	movieNumRating.put(movieID, movieNumRating.get(movieID)+1);
	        }else{
	        	movieRating.put(movieID, rating);
	        	movieNumRating.put(movieID, 1);
	        }
		}

		br.close();
		
		PrintWriter writer = new PrintWriter("intermediateFile", "UTF-8");
		
		for(Entry<Integer, Double> i:movieRating.entrySet()){
			writer.println(i.getKey()+"::"+(i.getValue()*1.0/movieNumRating.get(i.getKey())));
		}
		writer.close();
		
		movieRating.clear();
		movieNumRating.clear();
		
		aggregateRanges();
	}

	private static void aggregateRanges() throws NumberFormatException, IOException {
		int[] ranges = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		FileInputStream fis = new FileInputStream(new File("intermediateFile"));
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line;
		while((line = br.readLine()) != null) {
			String[] tokens = line.split("::");
			double averageRate = Double.parseDouble(tokens[1]);
	        
			if(averageRate>=0 && averageRate<0.5)
	        	ranges[0] ++;
	        else if(averageRate>=0.5 && averageRate< 1)
	        	ranges[1] ++;
	        else if(averageRate>=1 && averageRate< 1.5)
	        	ranges[2] ++;
	        else if(averageRate>=1.5 && averageRate< 2)
	        	ranges[3] ++;
	        else if(averageRate>=2 && averageRate< 2.5)
	        	ranges[4] ++;
	        else if(averageRate>=2.5 && averageRate< 3)
	        	ranges[5] ++;
	        else if(averageRate>=3&& averageRate< 3.5)
	        	ranges[6] ++;
	        else if(averageRate>=3.5 && averageRate< 4)
	        	ranges[7] ++;
	        else if(averageRate>=4 && averageRate< 4.5)
	        	ranges[8] ++;
	        else if(averageRate>=4.5 && averageRate<= 5)
	        	ranges[9] ++;
		}
		
		for(int i=0; i<10; i++)
			System.out.println(ranges[i]);
		
		br.close();
	}

}