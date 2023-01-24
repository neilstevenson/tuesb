package neil.demo;

import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.datamodel.Tuple2;

public class MyCallable implements Callable<String> {

	private static final int MAX = 100_000;
	private static final long SLOW_THRESHOLD_MS = 500L;
	
	private final int id;
	private final HazelcastInstance hazelcastInstance;
	private final TreeMap<String, String> treeMap;
	private final Random random = ThreadLocalRandom.current();
	
	public MyCallable(int id, HazelcastInstance hazelcastInstance, TreeMap<String, String> treeMap) {
		this.id = id;
		this.hazelcastInstance = hazelcastInstance;
		this.treeMap = treeMap;
	}

	@Override
	public String call() throws Exception {
		int bound = this.treeMap.size();
		@SuppressWarnings("unchecked")
		Tuple2<String, String>[] data = new Tuple2[bound];
		Object[] mapNames = this.treeMap.keySet().toArray();
		for (int i = 0; i < bound; i++) {
			data[i] = Tuple2.tuple2(mapNames[i].toString(), this.treeMap.get(mapNames[i]));
		}
		
		long sumNano = 0;
		long worstNano = Long.MIN_VALUE;
		long bestNano = Long.MAX_VALUE;
		long slow = 0;
		
		for (int i = (-1 * MAX); i < MAX; i++) {
			long beforeNano = System.nanoTime();
			long beforeMillis = System.currentTimeMillis();
			int j = this.random.nextInt(bound);
			String mapName = data[j].f0();
			String key = data[j].f1();
			
			@SuppressWarnings("unused")
			Object o = Objects.toString(this.hazelcastInstance.getMap(mapName).get(key));
			
			long elapsedNano = System.nanoTime() - beforeNano;
			long elapsedMillis = System.currentTimeMillis() - beforeMillis;
			// Validate
			//TimeUnit.NANOSECONDS.sleep(1L + this.random.nextInt(200));
			
			// i is negative, warm up ignored
			if (i >= 0) {
				sumNano += elapsedNano;
				if (elapsedNano > worstNano) {
					worstNano = elapsedNano;
				}
				if (elapsedNano < bestNano) {
					bestNano = elapsedNano;
				}
				if (elapsedMillis > SLOW_THRESHOLD_MS) {
					slow++;
				}
			}
			
			if (i % 10_000 == 0) {
				System.out.printf("%d - %s - count %d%n", this.id, this.getClass(), i); 
			}
		}
		
		long bestMs = TimeUnit.NANOSECONDS.toMillis(bestNano);
		long worstMs = TimeUnit.NANOSECONDS.toMillis(worstNano);
		double avgMs = TimeUnit.NANOSECONDS.toMillis(sumNano) / MAX;
		
		return String.format("%d - %s: best %d worst %d avg %f slow %d",
				this.id, this.getClass().getSimpleName(), bestMs, worstMs, avgMs, slow); 
	}

}
