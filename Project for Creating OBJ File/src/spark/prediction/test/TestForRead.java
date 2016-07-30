package spark.prediction.test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.prediction.TaskPrediction;
import org.junit.Test;


public class TestForRead {

	@Test
	public void testforead() throws Exception {
		HashMap<String,TaskPrediction>map=null;
		File savefile=new File("JavaTopK.obj");
		FileInputStream fi=new FileInputStream(savefile);
		ObjectInputStream ow=new ObjectInputStream(fi);
		map=(HashMap<String, TaskPrediction>) ow.readObject();
		ow.close();
		fi.close();
		Iterator<String> iterator = map.keySet().iterator();
		while(iterator.hasNext())
		{
			String key=iterator.next();
			
			
			System.out.println(key+" => "+map.get(key));
		}
	}
	
}
