package neil.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;

public class Application {
	private static final int NEAR_CACHE_SIZE = 50_000;
	private static final String MY_NAME = "tuesb";
	private static final TreeMap<String, String> treeMap = new TreeMap<>();
	
	public static void main(String[] args) throws Exception {
		String input = System.getProperty("MY_INPUT");
		String mapNameDefault = System.getProperty("MY_MAP_NAME", "");
		String nearCache = System.getProperty("MY_NEAR_CACHE", "");
		String nearCache2 = System.getProperty("MY_NEAR_CACHE2", "");
		
		ClientConfig clientConfig = new ClientConfig();

		clientConfig.setInstanceName(MY_NAME);
		
		clientConfig.getNetworkConfig().getKubernetesConfig()
		.setEnabled(true)
		.setProperty("service-dns", "neil-hazelcast.neil.svc.cluster.local");
		
		clientConfig.getNetworkConfig().getKubernetesConfig()
		.getProperties()
		.entrySet()
		.forEach(entry -> {
			System.out.printf("KUBERNETES CONFIG, '%s'=='%s'",
					entry.getKey(), entry.getValue());
		});
		
		if (nearCache.length() > 0) {
			EvictionConfig evictionConfig = new EvictionConfig();
			evictionConfig.setSize(NEAR_CACHE_SIZE);
			evictionConfig.setEvictionPolicy(EvictionPolicy.LFU);
			
			NearCacheConfig nearCacheConfig = new NearCacheConfig();
			nearCacheConfig.setEvictionConfig(evictionConfig);
			nearCacheConfig.setName(nearCache);
			
			clientConfig.getNearCacheConfigMap().put(nearCacheConfig.getName(), nearCacheConfig);
		}
		if (nearCache.length() > 0) {
			EvictionConfig evictionConfig2 = new EvictionConfig();
			evictionConfig2.setSize(NEAR_CACHE_SIZE);
			evictionConfig2.setEvictionPolicy(EvictionPolicy.LFU);
			
			NearCacheConfig nearCacheConfig2 = new NearCacheConfig();
			nearCacheConfig2.setEvictionConfig(evictionConfig2);
			nearCacheConfig2.setName(nearCache);
			
			clientConfig.getNearCacheConfigMap().put(nearCacheConfig2.getName(), nearCacheConfig2);
		}
		
		
		HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

    	System.out.println("START ------------" + new Date());
    	System.out.println("Input: " + Objects.toString(input));
    	System.out.println("MapName: '" + Objects.toString(mapNameDefault) + "'");
    	System.out.println("nearCache: '" + Objects.toString(nearCache) + "'");
    	System.out.println("nearCache2: '" + Objects.toString(nearCache2) + "'");
    	System.out.println("Map count " + hazelcastInstance.getDistributedObjects().size());
    	File file = new File(input);
    	
        try (BufferedReader bufferedReader =
                new BufferedReader(
                        new InputStreamReader(new FileInputStream(file)))) {
        	String line;
        	int count = 0;
        	while ((line = bufferedReader.readLine()) != null) {
            	String[] tokens = line.split(" ");
            	String mapName = tokens[1];
            	String key = tokens[2];
            	count++;
            	treeMap.put(mapName, key);
            }
        	
        	System.out.println("------------");
        	System.out.printf("Saved %d%n", count);
        	System.out.println("------------");
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        List<Callable<String>> callables = new ArrayList<>();
        int count = Integer.parseInt(System.getProperty("MY_COUNT"));
        System.out.println("@@@@@@@@@@@@@");
        System.out.println("@@@@@@@@@@@@@");
        System.out.println("@@@@@@@@@@@@@");
        System.out.println("Callables count == " + count);
        System.out.println("@@@@@@@@@@@@@");
        System.out.println("@@@@@@@@@@@@@");
        System.out.println("@@@@@@@@@@@@@");
        for (int i = 0; i < count; i++) {
        	callables.add(new MyCallable(i, hazelcastInstance, treeMap, mapNameDefault));
        }
        
        ExecutorService executorService = Executors.newFixedThreadPool(callables.size());
        executorService.invokeAll(callables).forEach(future -> {
        	try {
        		System.out.println(future.get());
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        });

    	System.out.println("END ------------" + new Date());
		TimeUnit.MINUTES.sleep(15L);
		hazelcastInstance.shutdown();
	}

}
