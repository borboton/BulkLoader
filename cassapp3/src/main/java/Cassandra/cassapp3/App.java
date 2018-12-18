package Cassandra.cassapp3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;

public class App {

	private final int threads;
	private final String[] contactHosts;
	
	
	public App(int threads, String... contactHosts) {
		this.threads = threads;
		this.contactHosts = contactHosts;
	}

	public void ingest(Iterator<Object[]> boundItemsIterator, String insertCQL) throws InterruptedException {
		
		Cluster cluster = Cluster.builder().addContactPoints(contactHosts).build();
		Session session = cluster.newSession();
		
		List<ResultSetFuture> futures = new ArrayList<>();
		
		final PreparedStatement statement = session.prepare(insertCQL);
		int count = 0;
		
		while (boundItemsIterator.hasNext()) {
			
			BoundStatement boundStatement = statement.bind(boundItemsIterator.next());
			ResultSetFuture future = session.executeAsync(boundStatement);					
			
			futures.add(future);
			count++;
			
			if (count % threads == 0) {
				futures.forEach(ResultSetFuture::getUninterruptibly);
				futures = new ArrayList<>();
			}
		}
	
		session.close();
		
		cluster.close();
		System.out.println("Close.");
	}

	public static void main(String[] args) throws InterruptedException {
		
		BasicConfigurator.configure();
		//String log4jproperties = "src/main/java/log4j.properties";
		String log4jproperties = "/scannet/conf/cassandra.properties";
		PropertyConfigurator.configure(log4jproperties);			
		
		Iterator<Object[]> rows = new Iterator<Object[]>() {
			int i = 0;
			Random random = new Random();

			@Override
			public boolean hasNext() {
				return i != 1000000;
			}

			@Override
			public Object[] next() {
				i++;
				return new Object[] { i, String.valueOf(random.nextLong()) };
			}
		};

		System.out.println("Starting benchmark");
		Stopwatch watch = Stopwatch.createStarted();
		
		//new App(8, "127.0.0.1").ingest(rows, "INSERT INTO my_test.rows (id, value) VALUES (?,?)");		
		new App(200, "10.11.34.185", "10.11.34.187").ingest(rows, "INSERT INTO kpscannet2.benchmark (id, value) VALUES ( ?, ?)");		 
		
		System.out.println("total time seconds = " + watch.elapsed(TimeUnit.SECONDS));
		
		watch.stop();
	}
    	
}
