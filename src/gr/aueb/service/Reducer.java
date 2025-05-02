package gr.aueb.service;

import gr.aueb.client.dtos.WorkerResultsMessage;
import gr.aueb.dtos.BaseReducerMessage;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reducer {

    private final int port;
    private final Map<String, List<String>> jobResults = new HashMap<>();
    private final Map<String, Integer> jobExpectedCounts = new HashMap<>();
    private final Map<String, Integer> jobReceivedCounts = new HashMap<>();
    private final Map<String, Object> jobLocks = new HashMap<>(); // Lock object per Job ID
    private final Map<String, Boolean> jobReadyFlags = new HashMap<>(); // Tracks if job results are ready
    private final Object allJobsLock = new Object(); // Global lock for accessing the state HashMaps

    // --- Server and Thread Management ---
    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;
    private final List<Thread> connectionThreads = Collections.synchronizedList(new ArrayList<>());

    public Reducer(int port) {
        this.port = port;
    }

    /**
     * Starts the Reducer server loop.
     */
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Reducer (Bare Minimum): Listening on port " + port + "...");

            while (isRunning) { // Simplified loop condition
                Socket connectionSocket = null;
                try {
                    connectionSocket = serverSocket.accept();
                    if (!isRunning) {
                        if (connectionSocket != null) try {
                            connectionSocket.close();
                        } catch (IOException e) {
                        }
                        break;
                    }
                    // Start a new thread for each connection
                    Socket finalConnectionSocket = connectionSocket;
                    Thread handlerThread = new Thread(() -> handleConnection(finalConnectionSocket));
                    handlerThread.start();

                } catch (SocketException e) {
                    if (!isRunning) break; // Expected when stopping
                    System.err.println("Reducer: Accept error: " + e.getMessage());
                } catch (IOException e) {
                    if (!isRunning) break;
                    System.err.println("Reducer: Accept IO error: " + e.getMessage());
                    // Ensure socket is closed on error
                    if (connectionSocket != null) try {
                        connectionSocket.close();
                    } catch (IOException ioEx) {
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Reducer: Could not start server on port " + port + ": " + e.getMessage());
        } finally {
            System.out.println("Reducer: Shutting down.");
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public void stop() {
        System.out.println("Reducer: Stop signal received.");
        isRunning = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
            }
        }
    }

    private void handleConnection(Socket connectionSocket) {
        try {
            InputStream initialInputStream = connectionSocket.getInputStream();
            PushbackInputStream pushbackInputStream = new PushbackInputStream(initialInputStream, 4);
            byte[] header = new byte[4];
            int bytesRead = pushbackInputStream.read(header);

            if (bytesRead == 4 && header[0] == (byte) 0xAC && header[1] == (byte) 0xED && header[2] == (byte) 0x00 && header[3] == (byte) 0x05) {
                // Worker (Object Stream)
                pushbackInputStream.unread(header);
                try (ObjectInputStream workerIn = new ObjectInputStream(pushbackInputStream)) {
                    handleWorkerCommunication(workerIn);
                }
            } else {
                // Master (Text Stream)
                if (bytesRead > 0) pushbackInputStream.unread(header, 0, bytesRead);
                try (BufferedReader masterIn = new BufferedReader(new InputStreamReader(pushbackInputStream));
                     PrintWriter masterOut = new PrintWriter(connectionSocket.getOutputStream(), true)) {
                    handleMasterCommunication(masterIn, masterOut);
                }
            }
        } catch (EOFException | SocketException e) {
            // Expected when client disconnects, ignore.
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Reducer Handler Error: " + e.getMessage());
        } finally {
            // Ensure socket is closed
            if (connectionSocket != null && !connectionSocket.isClosed()) {
                try {
                    connectionSocket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void handleMasterCommunication(BufferedReader reader, PrintWriter writer) throws IOException {
        String commandLine;
        while ((commandLine = reader.readLine()) != null) {
            // Minimal logging
            // System.out.println("Reducer < Master: " + commandLine);
            String[] parts = commandLine.trim().split("\\s+", 2);
            String command = parts[0].toUpperCase();
            String args = (parts.length > 1) ? parts[1] : "";

            switch (command) {
                case "START_REDUCE":
                    handleStartReduce(args);
                    break;
                case "GET_FINAL_RESULTS":
                    handleGetFinalResults(args, writer);
                    return; // Exit after handling result request
                default:
                    System.err.println("Reducer: Unknown Master command: " + command);
                    break;
            }
        }
    }

    private void handleWorkerCommunication(ObjectInputStream objIn) throws IOException, ClassNotFoundException {
        Object messageObject;
        while ((messageObject = objIn.readObject()) != null) {
            if (messageObject instanceof WorkerResultsMessage) {
                processWorkerResultsMessage((WorkerResultsMessage) messageObject);
            }
        }
    }

    /**
     * Handles START_REDUCE command.
     */
    private void handleStartReduce(String arguments) {
        String[] args = arguments.trim().split("\\s+");
        if (args.length < 2) return; // Silently ignore invalid command
        String jobId = args[0];
        try {
            int numWorkers = Integer.parseInt(args[1]);
            startNewJob(jobId, numWorkers);
        } catch (NumberFormatException e) {
            System.err.println("Reducer (START_REDUCE): Invalid number: " + args[1]);
        }
    }

    /**
     * Handles GET_FINAL_RESULTS command.
     */
    private void handleGetFinalResults(String jobId, PrintWriter writer) {
        jobId = jobId.trim();
        if (jobId.isEmpty()) {
            writer.println("ERROR_MISSING_JOB_ID");
        } else {
            List<String> finalResults = getFinalResults(jobId); // Call core logic
            if (finalResults != null) {
                for (String resultLine : finalResults) {
                    writer.println(resultLine);
                }
            } else {
                writer.println("ERROR_RESULTS_FAILED");
            }
        }
        writer.println("END_FINAL_RESULTS"); // Always send end marker
    }

    /**
     * Processes results from a worker.
     */
    private void processWorkerResultsMessage(WorkerResultsMessage resultsMsg) {
        String jobId = resultsMsg.getJobId();
        List<String> results = resultsMsg.getResults();
        if (jobId == null || results == null) return; // Ignore invalid messages

        Object lock = null;
        Integer expected = null;

        // Synchronize access to maps when retrieving job state
        synchronized (allJobsLock) {
            lock = jobLocks.get(jobId);
            expected = jobExpectedCounts.get(jobId);
        }

        if (lock == null || expected == null) return; // Job unknown or finished

        // Synchronize on the job-specific lock for state updates and notify
        synchronized (lock) {
            // Add results
            List<String> currentResults;
            synchronized (allJobsLock) {
                currentResults = jobResults.get(jobId);
                if (currentResults == null) return; // Job was cleaned up concurrently? Ignore.
            }
            currentResults.addAll(results); // Add to synchronized list

            // Increment received count
            int received = 0;
            synchronized (allJobsLock) {
                // Check if job still exists before incrementing
                if (!jobReceivedCounts.containsKey(jobId)) return; // Job cleaned up
                received = jobReceivedCounts.compute(jobId, (k, v) -> v + 1); // Assumes key exists
            }

            // Check if all expected messages are received
            if (received >= expected) {
                synchronized (allJobsLock) {
                    jobReadyFlags.put(jobId, true);
                }
                lock.notifyAll();
            }
        }
    }

    /**
     * Initializes state for a new job.
     */
    public boolean startNewJob(String jobId, int expectedWorkers) {
        if (jobId == null || jobId.trim().isEmpty() || expectedWorkers <= 0) return false;

        synchronized (allJobsLock) {
            if (jobExpectedCounts.containsKey(jobId)) return false; // Job exists
            Object lock = new Object();
            jobLocks.put(jobId, lock);
            jobExpectedCounts.put(jobId, expectedWorkers);
            jobReceivedCounts.put(jobId, 0);
            jobResults.put(jobId, Collections.synchronizedList(new ArrayList<>()));
            jobReadyFlags.put(jobId, false);
        }
        return true;
    }


    /**
     * Waits for job completion and retrieves results.
     */
    public List<String> getFinalResults(String jobId) {
        Object lock = null;
        Integer expected = null;

        synchronized (allJobsLock) {
            lock = jobLocks.get(jobId);
            expected = jobExpectedCounts.get(jobId);
        }

        if (lock == null || expected == null) return null; // Job doesn't exist

        List<String> results = null;
        try {
            synchronized (lock) {
                Boolean isReady = false;
                synchronized (allJobsLock) {
                    isReady = jobReadyFlags.getOrDefault(jobId, false);
                }

                while (!isReady) {
                    Integer currentReceived = null;
                    synchronized (allJobsLock) {
                        currentReceived = jobReceivedCounts.get(jobId);
                    }

                    // Check completion condition
                    if (currentReceived != null && expected != null && currentReceived >= expected) {
                        synchronized (allJobsLock) {
                            jobReadyFlags.put(jobId, true);
                        }
                        isReady = true;
                        break;
                    }
                    // Check if job was cleaned up while waiting
                    synchronized (allJobsLock) {
                        if (!jobLocks.containsKey(jobId)) return null;
                    }

                    lock.wait(); // Wait for notification
                    synchronized (allJobsLock) {
                        isReady = jobReadyFlags.getOrDefault(jobId, false);
                    }
                }
            }

            // Retrieve results (make a copy)
            synchronized (allJobsLock) {
                List<String> synchronizedList = jobResults.get(jobId);
                if (synchronizedList != null) {
                    synchronized (synchronizedList) {
                        results = new ArrayList<>(synchronizedList);
                    }
                } else {
                    results = new ArrayList<>();
                } // Return empty if null
            }
            cleanupJob(jobId); // Clean up after retrieval
            return results;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cleanupJob(jobId);
            return null;
        } catch (Exception e) { // Catch any other error during wait/retrieval
            System.err.println("Reducer (Job " + jobId + "): Error getting final results: " + e.getMessage());
            cleanupJob(jobId);
            return null;
        }
    }

    /**
     * Removes state associated with a completed/failed job.
     */
    private void cleanupJob(String jobId) {
        synchronized (allJobsLock) {
            // Minimal log
            // System.out.println("Reducer: Cleaning up Job ID: " + jobId);
            jobResults.remove(jobId);
            jobExpectedCounts.remove(jobId);
            jobReceivedCounts.remove(jobId);
            jobLocks.remove(jobId);
            jobReadyFlags.remove(jobId);
        }
    }
}