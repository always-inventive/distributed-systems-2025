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

/**
 * Reducer Node for the Distributed Food Delivery System.
 * Uses wait/notify for synchronization based on received result messages.
 * Uses synchronized HashMap for active jobs.
 * Handles connections directly using manual threads.
 * Listens for connections from Workers (Object Streams) and Master (Text Streams).
 * Avoids java.util.concurrent package and WorkerDoneMessage.
 * Reduced console output.
 */
public class Reducer {

    private final int port;
    private final Map<String, ReduceJobState> activeJobs = new HashMap<>();
    private final Object jobsMapLock = new Object(); // Lock for activeJobs map
    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;
    private final List<Thread> connectionThreads = Collections.synchronizedList(new ArrayList<>());

    /**
     * Represents the state of a single MapReduce job being processed by the Reducer.
     * Completion is determined by receiving the expected number of result messages.
     */
    private static class ReduceJobState {
        final String jobId;
        final int expectedWorkers; // Number of expected WorkerResultsMessages
        final List<String> intermediateResults = new ArrayList<>();
        // Synchronization primitives for completion
        private int resultsMessagesReceived = 0; // Counter for received result messages
        private boolean resultsReady = false; // Condition flag
        final Object completionLock = new Object(); // Monitor object for wait/notify
        final Object resultsListLock = new Object(); // Lock for intermediateResults list

        ReduceJobState(String jobId, int expectedWorkers) {
            this.jobId = jobId;
            this.expectedWorkers = expectedWorkers;
        }

        /**
         * Adds results from a worker and increments the received message count.
         * Notifies waiting threads if all expected messages are received.
         * Thread-safe access to the results list.
         */
        void addResultsAndNotify(List<String> workerResults) {
            synchronized (resultsListLock) {
                intermediateResults.addAll(workerResults);
            }
            synchronized (completionLock) {
                resultsMessagesReceived++;
                // Minimal printing inside the state object
                // System.out.println("Reducer (Job " + jobId + "): Results message received. Count: " + resultsMessagesReceived + "/" + expectedWorkers);
                if (resultsMessagesReceived >= expectedWorkers) {
                    resultsReady = true;
                    completionLock.notifyAll(); // Notify threads waiting on this job's completion
                }
            }
        }

        /**
         * Waits until all expected result messages have been received using wait/notify.
         *
         * @throws InterruptedException if the waiting thread is interrupted.
         */
        void awaitCompletion() throws InterruptedException {
            // Minimal printing
            System.out.println("Reducer (Job " + jobId + "): Waiting for results (Need " + expectedWorkers + " messages, Have " + resultsMessagesReceived + ")...");
            synchronized (completionLock) {
                while (!resultsReady) {
                    if (resultsMessagesReceived >= expectedWorkers) {
                        resultsReady = true;
                        break;
                    }
                    completionLock.wait(); // Wait for notification
                }
            }
            System.out.println("Reducer (Job " + jobId + "): Wait finished. Results are ready.");
        }

        /**
         * Gets the final aggregated results thread-safely.
         */
        List<String> getFinalResults() {
            synchronized (resultsListLock) {
                return new ArrayList<>(intermediateResults); // Return a copy
            }
        }
    }


    public Reducer(int port) {
        this.port = port;
    }

    /**
     * Starts the Reducer server loop.
     */
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Reducer: Listening on port " + port + "...");

            while (isRunning && !Thread.currentThread().isInterrupted()) {
                Socket connectionSocket = null;
                try {
                    connectionSocket = serverSocket.accept();
                    if (!isRunning) {
                        if (connectionSocket != null) connectionSocket.close();
                        break;
                    }
                    // Reduced printing: System.out.println("Reducer: New connection from " + connectionSocket.getInetAddress().getHostAddress());

                    Socket finalConnectionSocket = connectionSocket;
                    Thread handlerThread = new Thread(() -> {
                        try {
                            handleConnection(finalConnectionSocket);
                        } finally {
                            connectionThreads.remove(Thread.currentThread());
                            // Reduced printing: System.out.println("Reducer: Handler thread " + Thread.currentThread().getName() + " finished.");
                        }
                    }, "ReducerHandler-" + connectionSocket.getRemoteSocketAddress());

                    connectionThreads.add(handlerThread);
                    handlerThread.start();

                } catch (SocketException e) {
                    if (!isRunning) System.out.println("Reducer: Server socket closed, stopping acceptance.");
                    else System.err.println("Reducer: SocketException accepting connection: " + e.getMessage());
                } catch (IOException e) {
                    if (isRunning) System.err.println("Reducer: Error accepting connection: " + e.getMessage());
                    if (connectionSocket != null && !connectionSocket.isClosed()) {
                        try {
                            connectionSocket.close();
                        } catch (IOException ioEx) { /* Ignore */ }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Reducer: Could not start server on port " + port + ": " + e.getMessage());
        } finally {
            System.out.println("Reducer: Shutting down listener loop on port " + port + ".");
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) { /* Ignore */ }
            }
            waitForHandlerThreads();
            System.out.println("Reducer: Finished start method on port " + port + ".");
        }
    }

    /**
     * Signals the Reducer to stop gracefully.
     */
    public void stop() {
        System.out.println("Reducer: Received stop signal on port " + port + ".");
        isRunning = false;

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
                // Reduced printing: System.out.println("Reducer: Closed server socket on port " + port);
            } catch (IOException e) {
                System.err.println("Reducer: Error closing server socket during stop: " + e.getMessage());
            }
        }
        interruptHandlerThreads();
    }

    /**
     * Helper method to interrupt active handler threads.
     */
    private void interruptHandlerThreads() {
        synchronized (connectionThreads) {
            if (connectionThreads.isEmpty()) return;
            // Reduced printing: System.out.println("Reducer: Interrupting " + connectionThreads.size() + " active handler threads...");
            List<Thread> threadsToInterrupt = new ArrayList<>(connectionThreads);
            for (Thread t : threadsToInterrupt) {
                if (t != null && t.isAlive()) {
                    t.interrupt();
                }
            }
        }
    }

    /**
     * Helper method to wait for handler threads (best effort).
     */
    private void waitForHandlerThreads() {
        synchronized (connectionThreads) {
            if (connectionThreads.isEmpty()) return;
            // Reduced printing: System.out.println("Reducer: Waiting for " + connectionThreads.size() + " handler threads...");
            List<Thread> threadsToJoin = new ArrayList<>(connectionThreads);
            for (Thread t : threadsToJoin) {
                if (t != null && t.isAlive()) {
                    try {
                        t.join(1000);
                        if (t.isAlive()) {
                            System.err.println("Reducer: Handler thread " + t.getName() + " did not finish quickly.");
                        }
                    } catch (InterruptedException e) {
                        System.err.println("Reducer: Interrupted while waiting for handler thread " + t.getName());
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            connectionThreads.clear();
        }
        // Reduced printing: System.out.println("Reducer: Finished waiting for handler threads.");
    }


    /**
     * Handles a new incoming connection (Master or Worker).
     *
     * @param connectionSocket The socket representing the connection.
     */
    private void handleConnection(Socket connectionSocket) {
        String remoteAddress = connectionSocket.getInetAddress().getHostAddress();
        InputStream initialInputStream = null;
        OutputStream initialOutputStream = null;
        PushbackInputStream pushbackInputStream = null;
        ObjectInputStream workerIn = null;
        BufferedReader masterIn = null;
        PrintWriter masterOut = null;

        try {
            initialInputStream = connectionSocket.getInputStream();
            initialOutputStream = connectionSocket.getOutputStream();
            pushbackInputStream = new PushbackInputStream(initialInputStream, 4);
            byte[] header = new byte[4];
            int bytesRead = pushbackInputStream.read(header);

            if (bytesRead == 4 && header[0] == (byte) 0xAC && header[1] == (byte) 0xED && header[2] == (byte) 0x00 && header[3] == (byte) 0x05) {
                // Detected Worker (Object Stream)
                // Reduced printing: System.out.println("Handler: Connection from " + remoteAddress + " detected as Worker (Object Stream).");
                pushbackInputStream.unread(header);
                workerIn = new ObjectInputStream(pushbackInputStream);
                handleWorkerCommunication(workerIn);
            } else {
                // Assume Master (Text Stream)
                // Reduced printing: System.out.println("Handler: Connection from " + remoteAddress + " detected as Master (Text Stream).");
                if (bytesRead > 0) {
                    pushbackInputStream.unread(header, 0, bytesRead);
                }
                masterIn = new BufferedReader(new InputStreamReader(pushbackInputStream));
                masterOut = new PrintWriter(initialOutputStream, true);
                handleMasterCommunication(masterIn, masterOut);
            }
        } catch (StreamCorruptedException e) {
            // Reduced printing: System.out.println("Handler: StreamCorruptedException for " + remoteAddress + ". Assuming Master (Text Stream).");
            try {
                if (pushbackInputStream == null)
                    pushbackInputStream = new PushbackInputStream(connectionSocket.getInputStream());
                if (initialOutputStream == null) initialOutputStream = connectionSocket.getOutputStream();
                masterIn = new BufferedReader(new InputStreamReader(pushbackInputStream));
                masterOut = new PrintWriter(initialOutputStream, true);
                handleMasterCommunication(masterIn, masterOut);
            } catch (IOException ioException) {
                System.err.println("Handler: Error setting up Master streams after StreamCorruptedException for " + remoteAddress + ": " + ioException.getMessage());
            }
        } catch (SocketException e) {
            // Reduced printing: System.out.println("Handler: Connection " + remoteAddress + " closed or reset: " + e.getMessage());
        } catch (EOFException e) {
            // Reduced printing: System.out.println("Handler: Connection " + remoteAddress + " closed (EOF).");
        } catch (IOException | ClassNotFoundException e) {
            if (isRunning && !Thread.currentThread().isInterrupted()) {
                System.err.println("Handler: Error processing connection from " + remoteAddress + ": " + e.getMessage());
            }
        } catch (Exception e) {
            if (isRunning && !Thread.currentThread().isInterrupted()) {
                System.err.println("Handler: Unexpected error handling connection from " + remoteAddress + ": " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            // Reduced printing: System.out.println("Handler: Closing streams and socket for connection " + remoteAddress);
            if (masterOut != null) masterOut.close();
            try {
                if (workerIn != null) workerIn.close();
            } catch (IOException e) { /* Ignore */ }
            try {
                if (masterIn != null) masterIn.close();
            } catch (IOException e) { /* Ignore */ }
            try {
                if (connectionSocket != null && !connectionSocket.isClosed()) connectionSocket.close();
            } catch (IOException e) {
                System.err.println("Handler: Error closing socket (" + remoteAddress + "): " + e.getMessage());
            }
            // Reduced printing: System.out.println("Handler: Finished processing for " + remoteAddress);
        }
    }


    /**
     * Handles text-based communication with the Master node.
     *
     * @param reader BufferedReader connected to the Master.
     * @param writer PrintWriter connected to the Master.
     * @throws IOException If communication errors occur.
     */
    private void handleMasterCommunication(BufferedReader reader, PrintWriter writer) throws IOException {
        String commandLine;
        while (isRunning && !Thread.currentThread().isInterrupted() && (commandLine = reader.readLine()) != null) {
            // Reduced printing: System.out.println("Handler (Master): Received command: " + commandLine);
            String[] parts = commandLine.trim().split("\\s+", 2);
            String command = parts[0].toUpperCase();

            switch (command) {
                case "START_REDUCE":
                    if (parts.length > 1) handleStartReduce(parts[1]);
                    else System.err.println("Handler (Master - START_REDUCE): Missing arguments.");
                    break;
                case "GET_FINAL_RESULTS":
                    if (parts.length > 1) handleGetFinalResults(parts[1], writer);
                    else {
                        System.err.println("Handler (Master - GET_FINAL_RESULTS): Missing Job ID.");
                        writer.println("ERROR_MISSING_JOB_ID");
                        writer.println("END_FINAL_RESULTS");
                    }
                    return; // Assume Master disconnects after getting results
                default:
                    System.err.println("Handler (Master): Unknown command: " + command);
                    break;
            }
        }
        // Reduced printing: Log termination reason if needed
    }


    /**
     * Handles object-based communication with a Worker node.
     *
     * @param objIn ObjectInputStream connected to the Worker.
     * @throws IOException            If communication errors occur.
     * @throws ClassNotFoundException If a received object's class cannot be found.
     */
    private void handleWorkerCommunication(ObjectInputStream objIn) throws IOException, ClassNotFoundException {
        Object messageObject;
        while (isRunning && !Thread.currentThread().isInterrupted() && (messageObject = objIn.readObject()) != null) {
            if (messageObject instanceof BaseReducerMessage) {
                // Reduced printing: System.out.println("Handler (Worker): Received message: " + messageObject.getClass().getSimpleName());
                processReducerMessage((BaseReducerMessage) messageObject);
            } else {
                System.err.println("Handler (Worker): Received unknown object type: " + messageObject.getClass().getName());
            }
        }
        // Reduced printing: Log termination reason if needed
    }


    /**
     * Processes messages received from a Worker based on their specific type.
     * Only handles WorkerResultsMessage now.
     *
     * @param message The BaseReducerMessage received from the Worker.
     */
    private void processReducerMessage(BaseReducerMessage message) {
        if (message instanceof WorkerResultsMessage) {
            WorkerResultsMessage resultsMsg = (WorkerResultsMessage) message;
            // Reduced printing: System.out.println("Handler (Worker): Processing results for Job ID: " + resultsMsg.getJobId());
            addWorkerResultsAndNotifyCompletion(resultsMsg.getJobId(), resultsMsg.getResults()); // Call combined method
        }
        // Removed handling for WorkerDoneMessage
        else {
            System.err.println("Handler (Worker): Received unknown BaseReducerMessage type: " + message.getClass().getName());
        }
    }

    // --- Master Command Processing Methods ---

    /**
     * Handles START_REDUCE command from Master.
     */
    private void handleStartReduce(String arguments) {
        if (arguments == null || arguments.trim().isEmpty()) {
            System.err.println("Handler (START_REDUCE): Missing arguments.");
            return;
        }
        String[] args = arguments.trim().split("\\s+");
        if (args.length < 2) {
            System.err.println("Handler (START_REDUCE): Insufficient arguments. Expected: <job_id> <num_workers>. Got: " + arguments);
            return;
        }
        String jobId = args[0];
        try {
            int numWorkers = Integer.parseInt(args[1]);
            startNewJob(jobId, numWorkers); // Ignore return value for now
        } catch (NumberFormatException e) {
            System.err.println("Handler (START_REDUCE): Invalid number format: '" + args[1] + "'");
        }
    }

    /**
     * Handles GET_FINAL_RESULTS command from Master.
     */
    private void handleGetFinalResults(String jobId, PrintWriter writer) {
        jobId = jobId.trim();
        if (jobId.isEmpty()) {
            System.err.println("Handler (GET_FINAL_RESULTS): Empty Job ID.");
            writer.println("ERROR_MISSING_JOB_ID");
            writer.println("END_FINAL_RESULTS");
            return;
        }

        // Reduced printing: System.out.println("Handler: Received request for final results for Job ID: " + jobId);
        List<String> finalResults = getFinalResults(jobId);

        if (finalResults != null) {
            // Reduced printing: System.out.println("Handler: Sending " + finalResults.size() + " final results for Job ID: " + jobId);
            for (String resultLine : finalResults) {
                writer.println(resultLine);
            }
        } else {
            // Reduced printing: System.out.println("Handler: Failed to retrieve results for Job ID: " + jobId + ". Sending error.");
            writer.println("ERROR_RESULTS_FAILED");
        }
        writer.println("END_FINAL_RESULTS");
        // Reduced printing: System.out.println("Handler: Finished sending results/response for Job ID: " + jobId);
    }


    // --- Core Reducer Logic Methods ---

    /**
     * Starts tracking a new MapReduce job.
     */
    public boolean startNewJob(String jobId, int expectedWorkers) {
        synchronized (jobsMapLock) {
            if (activeJobs.containsKey(jobId)) {
                System.err.println("Reducer: Job ID " + jobId + " already exists.");
                return false;
            }
            if (expectedWorkers <= 0) {
                System.err.println("Reducer: Invalid expected workers (" + expectedWorkers + ") for Job ID " + jobId);
                return false;
            }
            ReduceJobState newJob = new ReduceJobState(jobId, expectedWorkers);
            activeJobs.put(jobId, newJob);
            // Reduced printing: System.out.println("Reducer: Started new Job ID: " + jobId + ", Expecting Messages: " + expectedWorkers);
            return true;
        }
    }

    /**
     * Adds intermediate results and notifies completion logic.
     * Renamed from addWorkerResults.
     */
    public void addWorkerResultsAndNotifyCompletion(String jobId, List<String> workerResults) {
        ReduceJobState jobState;
        synchronized (jobsMapLock) {
            jobState = activeJobs.get(jobId);
        }
        if (jobState != null) {
            jobState.addResultsAndNotify(workerResults); // Call the state's method
            // Reduced printing: System.out.println("Reducer (Job " + jobId + "): Added " + (workerResults != null ? workerResults.size() : 0) + " results.");
        } else {
            System.err.println("Reducer: Received results for unknown/completed Job ID: " + jobId + ". Ignoring.");
        }
    }

    // Removed notifyWorkerFinished method

    /**
     * Waits for job completion and retrieves final results.
     */
    public List<String> getFinalResults(String jobId) {
        ReduceJobState jobState;
        synchronized (jobsMapLock) {
            jobState = activeJobs.get(jobId);
            if (jobState == null) {
                System.err.println("Reducer: Request for final results of unknown/completed Job ID: " + jobId);
                return null;
            }
            try {
                jobState.awaitCompletion();
                List<String> finalResults = jobState.getFinalResults();
                // Reduced printing: System.out.println("Reducer (Job " + jobId + "): Retrieving final results (" + finalResults.size() + ").");
                activeJobs.remove(jobId); // Clean up completed job
                // Reduced printing: System.out.println("Reducer (Job " + jobId + "): Job state cleaned up.");
                return finalResults;
            } catch (InterruptedException e) {
                System.err.println("Reducer (Job " + jobId + "): Wait for final results interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
                activeJobs.remove(jobId); // Clean up potentially incomplete job
                return null;
            } catch (Exception e) {
                System.err.println("Reducer (Job " + jobId + "): Error retrieving final results: " + e.getMessage());
                e.printStackTrace();
                activeJobs.remove(jobId); // Clean up job on error
                return null;
            }
        }
    }
}
