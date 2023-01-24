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
import com.hazelcast.core.HazelcastInstance;

public class Application {
	private static final String MY_NAME = "tuesb";
	private static final TreeMap<String, String> treeMap = new TreeMap<>();
	
	public static void main(String[] args) throws Exception {
		String input = System.getProperty("MY_INPUT");

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
		
		HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

    	System.out.println("START ------------" + new Date());
    	System.out.println("Input: " + Objects.toString(input));
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
        	callables.add(new MyCallable(i, hazelcastInstance, treeMap));
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
