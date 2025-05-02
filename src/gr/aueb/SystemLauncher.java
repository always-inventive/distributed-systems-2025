package gr.aueb;

import gr.aueb.service.Master;
import gr.aueb.service.Reducer;
import gr.aueb.service.Worker;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A utility class to launch the Reducer, Worker(s), and Master components
 * as threads within a single JVM process using basic java.lang.Thread.
 * It establishes connections to Workers and injects them into the Master.
 */
public class SystemLauncher {

    private static final String DEFAULT_CONFIG_FILE = "resources\\system.config";
    private static final List<Closeable> closeableResources = Collections.synchronizedList(new ArrayList<>());
    private static final List<Thread> componentThreads = Collections.synchronizedList(new ArrayList<>());
    private static Master masterInstance;
    private static Reducer reducerInstance;
    private static final List<Worker> workerInstances = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        String configFile = DEFAULT_CONFIG_FILE;
        if (args.length > 0) {
            configFile = args[0];
        }
        System.out.println("System Launcher: Using configuration file: " + configFile);
        Properties config = loadConfiguration(configFile);
        if (config == null) {
            System.err.println("System Launcher: Failed to load configuration. Exiting.");
            return;
        }

        // Add shutdown hook to terminate components
        Runtime.getRuntime().addShutdownHook(new Thread(SystemLauncher::shutdownSystem));

        try {
            // Extract Configuration
            String reducerHost = config.getProperty("reducer.host", "127.0.0.1");
            int reducerPort = Integer.parseInt(config.getProperty("reducer.port"));
            int masterPort = Integer.parseInt(config.getProperty("master.port", "5000"));

            // Parse Worker Configurations
            List<Map<String, Object>> workerConfigs = parseWorkerConfigsFromProps(config);

            // --- Start Components as Threads ---

            // 1. Start Reducer Thread
            System.out.println("\n--- Starting Reducer on port " + reducerPort + " ---");
            reducerInstance = new Reducer(reducerPort);
            Thread reducerThread = new Thread(reducerInstance::start, "ReducerThread-" + reducerPort);
            componentThreads.add(reducerThread);
            reducerThread.start();
            Thread.sleep(500); // Allow socket to bind

            // 2. Start Worker Threads
            workerInstances.clear();
            for (Map<String, Object> workerConfig : workerConfigs) {
                int workerPort = (int) workerConfig.get("port");
                System.out.println("\n--- Starting Worker on port " + workerPort + " (Reducer: " + reducerHost + ":" + reducerPort + ") ---");
                Worker worker = new Worker(workerPort, reducerHost, reducerPort);
                workerInstances.add(worker);
                Thread workerThread = new Thread(worker, "WorkerThread-" + workerPort);
                componentThreads.add(workerThread);
                workerThread.start();
                Thread.sleep(200); // Allow time for worker to start listening
            }
            System.out.println("System Launcher: Waiting for workers to initialize...");
            Thread.sleep(2000); // Allow time for all workers to start listening

            // 3. Establish Master -> Worker Connections
            System.out.println("\n--- Establishing Master -> Worker Connections ---");
            Map<Integer, Master.WorkerConnection> establishedConnections = new HashMap<>();
            for (Map<String, Object> workerConfig : workerConfigs) {
                int id = (int) workerConfig.get("id");
                String ip = (String) workerConfig.get("ip");
                int port = (int) workerConfig.get("port");
                try {
                    System.out.println("System Launcher: Connecting Master to Worker " + id + " @ " + ip + ":" + port);
                    Socket workerSocket = new Socket(ip, port);
                    Master.WorkerConnection connection = new Master.WorkerConnection(workerSocket, id, ip, port);
                    establishedConnections.put(id, connection);
                    closeableResources.add(connection);
                    System.out.println("System Launcher: Connection to Worker " + id + " established.");
                } catch (IOException e) {
                    System.err.println("System Launcher: FATAL - Failed to connect Master to Worker " + id + " @ " + ip + ":" + port + " - " + e.getMessage());
                    shutdownSystem();
                    return;
                }
            }

            // 4. Configure and Start Master Thread
            System.out.println("\n--- Configuring and Starting Master ---");
            masterInstance = new Master();
            masterInstance.configure(masterPort, reducerHost, reducerPort, establishedConnections);
            Thread masterThread = new Thread(masterInstance, "MasterThread-" + masterPort);
            componentThreads.add(masterThread);
            masterThread.start();

            System.out.println("\n--- System components initialized and running ---");
            System.out.println("Launcher thread finished initialization. System running in background threads.");
            System.out.println("Press Ctrl+C to trigger shutdown hook.");

        } catch (NumberFormatException e) {
            System.err.println("System Launcher: Invalid port number in configuration file.");
            shutdownSystem();
        } catch (InterruptedException e) {
            System.err.println("System Launcher: Launch sequence interrupted.");
            Thread.currentThread().interrupt();
            shutdownSystem();
        } catch (Exception e) {
            System.err.println("System Launcher: An unexpected error occurred during launch: " + e.getMessage());
            e.printStackTrace();
            shutdownSystem();
        }
    }

    /**
     * Loads configuration from the specified properties file.
     */
    private static Properties loadConfiguration(String filename) {
        Properties props = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(filename))) {
            props.load(input);
            return props;
        } catch (IOException e) {
            System.err.println("System Launcher: Error reading config file '" + filename + "': " + e.getMessage());
            return null;
        }
    }

    /**
     * Parses worker configurations from Properties
     */
    private static List<Map<String, Object>> parseWorkerConfigsFromProps(Properties props) {
        List<Map<String, Object>> workerConfigs = new ArrayList<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("worker.")) {
                try {
                    int id = Integer.parseInt(key.substring("worker.".length()));
                    String value = props.getProperty(key);
                    String[] parts = value.split(":");
                    if (parts.length == 2) {
                        String ip = parts[0].trim();
                        int port = Integer.parseInt(parts[1].trim());
                        workerConfigs.add(Map.of("id", id, "ip", ip, "port", port));
                    } else {
                        System.err.println("System Launcher: Invalid format for worker key '" + key + "'");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("System Launcher: Invalid ID or Port for worker key '" + key + "'");
                } catch (Exception e) {
                    System.err.println("System Launcher: Error parsing worker key '" + key + "': " + e.getMessage());
                }
            }
        }
        if (!workerConfigs.isEmpty()) {
            workerConfigs.sort((m1, m2) -> Integer.compare((int) m1.get("id"), (int) m2.get("id")));
        }
        return workerConfigs;
    }


    /**
     * Attempts to shut down all components gracefully.
     * Called by the shutdown hook. Uses basic Thread interruption and join.
     */
    private static void shutdownSystem() {
        System.out.println("\n--- System Launcher: Shutting down system components... ---");

        // 1. Signal Master to stop accepting clients and close worker connections
        if (masterInstance != null) {
            System.out.println("System Launcher: Shutting down Master...");
            masterInstance.shutdownWorkers();
        }

        // 2. Signal Reducer thread to stop listening
        if (reducerInstance != null) {
            System.out.println("System Launcher: Stopping Reducer...");
            reducerInstance.stop(); // Signals the loop and closes server socket
        }

        // 3. Signal Worker threads to stop listening
        System.out.println("System Launcher: Stopping Workers...");
        synchronized (workerInstances) {
            for (Worker worker : workerInstances) {
                worker.stop(); // Signals the loop and closes server socket
            }
        }

        // 4. Close Master -> Worker connections established by Launcher
        System.out.println("System Launcher: Closing established Master->Worker connections...");
        synchronized (closeableResources) {
            for (Closeable resource : closeableResources) {
                try {
                    System.out.println("System Launcher: Closing " + resource.getClass().getSimpleName() + "...");
                    resource.close();
                } catch (IOException e) {
                    System.err.println("System Launcher: Error closing resource: " + e.getMessage());
                }
            }
            closeableResources.clear();
        }

        // 5. Interrupt and wait for component threads to finish (best effort)
        System.out.println("System Launcher: Interrupting and joining component threads...");
        synchronized (componentThreads) {
            for (Thread t : componentThreads) {
                if (t != null && t.isAlive()) {
                    System.out.println("System Launcher: Interrupting thread " + t.getName() + "...");
                    t.interrupt(); // Signal interruption
                }
            }
            // Wait for threads to die (with a timeout)
            for (Thread t : componentThreads) {
                if (t != null && t.isAlive()) {
                    try {
                        System.out.println("System Launcher: Waiting for thread " + t.getName() + " to finish...");
                        t.join(2000); // Wait up to 2 seconds
                        if (t.isAlive()) {
                            System.err.println("System Launcher: Thread " + t.getName() + " did not terminate after interruption and join timeout.");
                        } else {
                            System.out.println("System Launcher: Thread " + t.getName() + " finished.");
                        }
                    } catch (InterruptedException e) {
                        System.err.println("System Launcher: Interrupted while joining thread " + t.getName());
                        Thread.currentThread().interrupt(); // Preserve interrupt status
                    }
                }
            }
            componentThreads.clear();
        }

        System.out.println("--- System Launcher: Shutdown sequence complete ---");
    }
}