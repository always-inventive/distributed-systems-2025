package gr.aueb.service;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException; // Import SocketTimeoutException
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import gr.aueb.client.ClientRequestHandler;
import gr.aueb.client.dtos.*;
import gr.aueb.dtos.BaseRequest;
import gr.aueb.manager.ManagerRequestHandler;
import gr.aueb.manager.dtos.BaseManagerRequest; // Import BaseManagerRequest
import gr.aueb.model.FilterCriteria;

/**
 * Master Node for the Distributed Food Delivery System.
 * Receives configuration and established WorkerConnections via its constructor or setters.
 * Uses Object Streams for Master <-> Worker communication.
 * Listens for client connections, identifies client type based on the first request object,
 * and starts the appropriate handler (ClientRequestHandler or ManagerRequestHandler).
 * Designed to be run as an object within a larger application/launcher.
 */
public class Master implements Runnable { // Implement Runnable to be started in a thread

    // Stores active connections to workers (Worker ID -> WorkerConnection)
    private final Map<Integer, WorkerConnection> workers = new ConcurrentHashMap<>();
    // Store worker IDs in a sorted list for consistent hashing
    private List<Integer> sortedWorkerIds = new ArrayList<>();

    // Configuration details (to be set by the launcher)
    private String reducerHost;
    private int reducerPort;
    private int masterListenPort; // Port for Master to listen on

    /**
     * Manages the TCP connection and communication with a single Worker node
     * using Object Streams.
     */
    public static class WorkerConnection implements Closeable {
        private final Socket socket;
        private ObjectOutputStream objectOut;
        private ObjectInputStream objectIn;
        private final int workerId;
        private final String ip;
        private final int port;
        private final Object outputStreamLock = new Object(); // Lock for sending requests to this worker
        private final Object inputStreamLock = new Object(); // Lock for reading responses from this worker

        public WorkerConnection(Socket socket, int workerId, String ip, int port) throws IOException {
            this.socket = socket;
            this.workerId = workerId;
            this.ip = ip;
            this.port = port;
            // Initialize streams immediately
            // IMPORTANT: Output stream MUST be initialized first
            this.objectOut = new ObjectOutputStream(socket.getOutputStream());
            this.objectIn = new ObjectInputStream(socket.getInputStream());
            System.out.println("Master: WorkerConnection streams initialized for Worker " + workerId + " @ " + ip + ":" + port);
        }

        /**
         * Sends a request object to the worker. Synchronized on outputStreamLock.
         * @param request The BaseRequest object to send.
         * @throws IOException If an I/O error occurs.
         */
        public void sendRequest(BaseRequest request) throws IOException {
            synchronized (outputStreamLock) {
                System.out.println("Master -> Worker " + workerId + ": Sending " + request.getClass().getSimpleName() + "...");
                objectOut.writeObject(request);
                objectOut.flush(); // Ensure data is sent immediately
                // Reset is important to handle object graph cycles if the same object is sent multiple times
                objectOut.reset();
            }
        }

        /**
         * Reads a response object from the worker. Synchronized on inputStreamLock.
         * @return The received Serializable object.
         * @throws IOException If an I/O error occurs.
         * @throws ClassNotFoundException If the class of the serialized object cannot be found.
         */
        public Object readResponse() throws IOException, ClassNotFoundException {
            synchronized (inputStreamLock) {
                // System.out.println("Master <- Worker " + workerId + ": Reading response..."); // Optional: Verbose logging
                return objectIn.readObject();
            }
        }

        @Override
        public void close() {
            System.out.println("Master: Closing WorkerConnection for Worker " + workerId + " @ " + ip + ":" + port);
            // Close streams first
            try {
                if (objectOut != null) objectOut.close();
            } catch (IOException e) { /* Ignore */ }
            try {
                if (objectIn != null) objectIn.close();
            } catch (IOException e) { /* Ignore */ }
            // Then close the socket
            try {
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (IOException e) {
                System.err.println("Master: Error closing connection socket to Worker " + workerId + ": " + e.getMessage());
            }
        }

        // Getters
        public int getWorkerId() { return workerId; }
        public String getIp() { return ip; }
        public int getPort() { return port; }
    }

    /**
     * Default constructor
     */
    public Master() {
    }

    /**
     * Configures the Master with necessary details before starting.
     *
     * @param masterListenPort   Port the Master should listen on for clients.
     * @param reducerHost        Reducer's hostname or IP.
     * @param reducerPort        Reducer's port.
     * @param establishedWorkers A map of Worker ID to established WorkerConnection.
     */
    public void configure(int masterListenPort, String reducerHost, int reducerPort, Map<Integer, WorkerConnection> establishedWorkers) {
        this.masterListenPort = masterListenPort;
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;

        if (establishedWorkers == null || establishedWorkers.isEmpty()) {
            throw new IllegalArgumentException("Master requires at least one established worker connection.");
        }

        this.workers.putAll(establishedWorkers);

        // Populate and sort worker IDs for consistent hashing
        List<Integer> ids = new ArrayList<>(establishedWorkers.keySet());
        Collections.sort(ids);
        this.sortedWorkerIds = Collections.unmodifiableList(ids);

        System.out.println("Master configured:");
        System.out.println("  Listening Port: " + this.masterListenPort);
        System.out.println("  Reducer: " + this.reducerHost + ":" + this.reducerPort);
        System.out.println("  Workers Connected: " + this.workers.size() + ", Sorted IDs: " + this.sortedWorkerIds);
    }


    /**
     * The main loop for the Master thread, starts the server.
     * Called when the Master thread starts.
     */
    @Override
    public void run() {
        if (masterListenPort <= 0 || workers.isEmpty()) {
            System.err.println("Master cannot start. Configuration is incomplete (port=" + masterListenPort + ", workers=" + workers.size() + "). Call configure() first.");
            return;
        }
        startServer(this.masterListenPort);
    }


    /**
     * Starts the TCP server to listen for incoming client connections.
     * Reads the first object sent by the client to determine its type
     * (BaseClientRequest or BaseManagerRequest) and starts the appropriate handler.
     *
     * @param port The port number to listen on.
     */
    public void startServer(int port) {
        ServerSocket serverSocket = null; // Declare outside try to close in finally
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Master: Listening for client connections on port " + port + "...");

            while (!Thread.currentThread().isInterrupted()) { // Check for interruption for graceful shutdown
                Socket clientSocket = null;
                ObjectOutputStream objOut = null;
                ObjectInputStream objIn = null;
                Object firstRequest = null; // To store the first request object

                try {
                    clientSocket = serverSocket.accept(); // Blocks here
                    String clientAddress = clientSocket.getInetAddress().getHostAddress();
                    System.out.println("Master: New connection from " + clientAddress);

                    // Set a timeout for reading the first object to prevent blocking indefinitely
                    clientSocket.setSoTimeout(100000); // 10 seconds timeout

                    // --- Initialize Object Streams ---
                    // IMPORTANT: Output stream MUST be initialized first to avoid deadlock
                    objOut = new ObjectOutputStream(clientSocket.getOutputStream());
                    objIn = new ObjectInputStream(clientSocket.getInputStream());
                    System.out.println("Master: Initialized Object Streams for " + clientAddress);

                    // --- Read the first request object to identify client type ---
                    System.out.println("Master: Reading first request object from " + clientAddress + "...");
                    firstRequest = objIn.readObject();
                    System.out.println("Master: Received first object of type: " + (firstRequest != null ? firstRequest.getClass().getName() : "null"));

                    // Disable the timeout after the first read
                    clientSocket.setSoTimeout(0);

                    Runnable handler = null;
                    // --- Identify Client Type based on the first object ---
                    if (firstRequest instanceof BaseClientRequest) {
                        System.out.println("Master: Identified connection as REGULAR CLIENT based on first request.");
                        // Pass the first request to the handler's constructor
                        handler = new ClientRequestHandler(clientSocket, this, objIn, objOut, (BaseClientRequest) firstRequest);
                    } else if (firstRequest instanceof BaseManagerRequest) {
                        System.out.println("Master: Identified connection as MANAGER CLIENT based on first request.");
                        // Pass the first request to the handler's constructor
                        handler = new ManagerRequestHandler(clientSocket, this, objIn, objOut, (BaseManagerRequest) firstRequest);
                    } else {
                        // Handle cases where the first object is not a recognized request type
                        System.err.println("Master: Received unknown first object type: '" + (firstRequest != null ? firstRequest.getClass().getName() : "null") + "' from " + clientAddress);
                        // Optionally send an error response if possible, though the protocol might not support it here.
                    }

                    // Start the appropriate handler thread if identified
                    if (handler != null) {
                        Thread handlerThread = new Thread(handler);
                        // Optional: Set thread name for easier debugging
                        handlerThread.setName((firstRequest instanceof BaseClientRequest ? "ClientHandler-" : "ManagerHandler-") + clientAddress);
                        handlerThread.start(); // Start handler in its own thread
                    } else {
                        // If no handler was created (unknown type), close the connection
                        System.err.println("Master: Closing unidentified connection from " + clientAddress);
                        if (objOut != null) try { objOut.close(); } catch (IOException ioex) { /* Ignore */ }
                        if (objIn != null) try { objIn.close(); } catch (IOException ioex) { /* Ignore */ }
                        if (clientSocket != null) try { clientSocket.close(); } catch (IOException ioex) { /* Ignore */ }
                    }

                } catch (SocketTimeoutException e) {
                    System.err.println("Master: Timeout waiting for first request object from " + (clientSocket != null ? clientSocket.getInetAddress().getHostAddress() : "unknown") + ". Closing connection.");
                    // Close resources if timeout occurs before identification
                    if (objOut != null) try { objOut.close(); } catch (IOException ioex) { /* Ignore */ }
                    if (objIn != null) try { objIn.close(); } catch (IOException ioex) { /* Ignore */ }
                    if (clientSocket != null) try { clientSocket.close(); } catch (IOException ioex) { /* Ignore */ }
                } catch (EOFException e) {
                    System.out.println("Master: Client " + (clientSocket != null ? clientSocket.getInetAddress().getHostAddress() : "unknown") + " disconnected before sending first request object (EOF).");
                    // Resources are likely already closed or will be handled by finally
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Master: Error during client connection setup or first object read: " + e.getMessage());
                    // Close resources on error
                    if (objOut != null) try { objOut.close(); } catch (IOException ioex) { /* Ignore */ }
                    if (objIn != null) try { objIn.close(); } catch (IOException ioex) { /* Ignore */ }
                    if (clientSocket != null) try { clientSocket.close(); } catch (IOException ioex) { /* Ignore */ }
                } catch (Exception e) { // Catch any other unexpected errors
                    System.err.println("Master: Unexpected error during client connection handling: " + e.getMessage());
                    e.printStackTrace();
                    // Close resources on error
                    if (objOut != null) try { objOut.close(); } catch (IOException ioex) { /* Ignore */ }
                    if (objIn != null) try { objIn.close(); } catch (IOException ioex) { /* Ignore */ }
                    if (clientSocket != null) try { clientSocket.close(); } catch (IOException ioex) { /* Ignore */ }
                }
            } // End while loop
        } catch (SocketException e) {
            // Handle expected exception when serverSocket is closed by shutdown
            if ("Socket closed".equals(e.getMessage()) || "Socket operation on closed socket".contains(e.getMessage())) {
                System.out.println("Master: Server socket closed, stopping listening.");
            } else {
                System.err.println("Master: SocketException on server socket: " + e.getMessage());
            }
        } catch (IOException e) {
            System.err.println("Master: Could not start server on port " + port + ": " + e.getMessage());
        } finally {
            System.out.println("Master: Server loop finished.");
            // Close the server socket if it's still open
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    System.err.println("Master: Error closing server socket in finally block: " + e.getMessage());
                }
            }
            // Ensure workers are closed when Master stops listening
            // Note: shutdownWorkers() might be called by the launcher's shutdown hook as well.
            // Calling it here ensures cleanup if the Master loop exits unexpectedly.
            shutdownWorkers();
        }
    }

    /**
     * Selects the appropriate worker ID for a given store name using consistent hashing.
     *
     * @param storeName The name of the store.
     * @return The ID of the worker responsible for this store.
     * @throws IllegalStateException if no workers are configured.
     */
    public int getWorkerIdForStore(String storeName) {
        if (sortedWorkerIds == null || sortedWorkerIds.isEmpty()) {
            throw new IllegalStateException("No workers available/configured.");
        }
        // Simple consistent hashing: hash the store name and map to a worker index
        int workerIndex = Math.abs(storeName.hashCode()) % sortedWorkerIds.size();
        return sortedWorkerIds.get(workerIndex);
    }

    /**
     * Forwards a Serializable request object (must extend BaseRequest) to a specific worker.
     *
     * @param targetWorkerId The ID of the worker to send the request to.
     * @param request        The BaseRequest object to send.
     * @return true if the request was sent successfully, false otherwise.
     */
    public boolean forwardRequestObjectToWorker(int targetWorkerId, BaseRequest request) {
        WorkerConnection connection = workers.get(targetWorkerId);
        if (connection != null) {
            try {
                connection.sendRequest(request);
                return true;
            } catch (IOException e) {
                System.err.println("Master: Error sending request object ("+ request.getClass().getSimpleName() +") to Worker " + targetWorkerId + ": " + e.getMessage());
                // Consider removing or marking the worker connection as faulty here
                return false;
            } catch (Exception e) { // Catch other potential runtime errors
                System.err.println("Master: Unexpected error sending request object ("+ request.getClass().getSimpleName() +") to Worker " + targetWorkerId + ": " + e.getMessage());
                return false;
            }
        } else {
            System.err.println("Master: Worker connection with ID " + targetWorkerId + " not found for forwarding request object ("+ request.getClass().getSimpleName() +").");
            return false;
        }
    }


    /**
     * Orchestrates the MapReduce process for filtering stores via the Reducer.
     * Sends FilterRequest objects to all workers and retrieves final results from the Reducer.
     *
     * @param filterCriteria The criteria object provided by the client.
     * @return A List of strings representing the filtered stores, or an empty list if errors occur.
     */
    public List<String> performMapReduceFilterViaReducer(FilterCriteria filterCriteria) {
        String jobId = "filter-job-" + UUID.randomUUID().toString(); // Unique Job ID
        int numWorkers = workers.size();
        List<String> finalResults = new ArrayList<>();

        // --- Input Validation ---
        if (filterCriteria == null) {
            System.err.println("Master (Job " + jobId + "): Null filter criteria received for MapReduce.");
            return finalResults; // Return empty list
        }
        if (numWorkers == 0) {
            System.err.println("Master (Job " + jobId + "): No connected workers to perform MapReduce.");
            return finalResults; // Return empty list
        }
        if (reducerHost == null || reducerPort <= 0) {
            System.err.println("Master (Job " + jobId + "): Reducer not configured.");
            return finalResults; // Return empty list
        }

        System.out.println("Master: Starting MapReduce Filter (Job: " + jobId + ") via Reducer (" + reducerHost + ":" + reducerPort + ")");

        // --- 1. Notify Reducer to Start Job (Text Protocol) ---
        try (Socket reducerSocket = new Socket(reducerHost, reducerPort);
             PrintWriter reducerOut = new PrintWriter(reducerSocket.getOutputStream(), true)) { // Auto-flush
            System.out.println("Master -> Reducer: Notifying START_REDUCE for Job ID: " + jobId + " (Workers: " + numWorkers + ")");
            reducerOut.println("START_REDUCE " + jobId + " " + numWorkers);
            // Connection closes automatically here (try-with-resources)
        } catch (IOException e) {
            System.err.println("Master (Job " + jobId + "): Failed to communicate with Reducer (START_REDUCE): " + e.getMessage());
            return finalResults; // Cannot proceed without Reducer confirmation
        }

        // --- 2. Send Map Tasks to Workers (Object Protocol) ---
        System.out.println("Master (Job " + jobId + "): Sending Map tasks (FilterStoresClientRequest) to Workers...");
        // Create the request object to send to each worker
        // Note: This DTO name 'FilterStoresClientRequest' is confusing here, it's Master->Worker
        // A better name might be 'MapFilterTaskRequest' or similar. Using existing for now.
        MapFilterTaskRequest mapRequestObject = new MapFilterTaskRequest(jobId, reducerHost, reducerPort, filterCriteria);

        // Send request to all workers concurrently using simple Threads
        List<Thread> workerMapThreads = new ArrayList<>();
        for (WorkerConnection worker : workers.values()) {
            final WorkerConnection targetWorker = worker; // Final variable for lambda
            Thread t = new Thread(() -> {
                try {
                    System.out.println("Master (Job " + jobId + ") -> Worker " + targetWorker.getWorkerId() + ": Sending MapFilterTaskRequest...");
                    targetWorker.sendRequest(mapRequestObject);
                } catch (IOException e) {
                    // Log error but continue trying other workers
                    System.err.println("Master (Job " + jobId + "): Communication error sending Map task to Worker " + targetWorker.getWorkerId() + ": " + e.getMessage());
                    // TODO: Consider adding logic to notify Reducer about worker failure?
                }
            });
            workerMapThreads.add(t);
            t.start();
        }

        // Optional: Wait for all map task sending threads to complete (not strictly necessary)
        // for (Thread t : workerMapThreads) { try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }

        // --- 3. Request Final Results from Reducer (Text Protocol) ---
        System.out.println("Master (Job " + jobId + "): Requesting final results from Reducer...");
        try (Socket reducerSocket = new Socket(reducerHost, reducerPort);
             PrintWriter reducerOut = new PrintWriter(reducerSocket.getOutputStream(), true); // Auto-flush
             BufferedReader reducerIn = new BufferedReader(new InputStreamReader(reducerSocket.getInputStream()))) {

            reducerOut.println("GET_FINAL_RESULTS " + jobId); // Send command

            // Read results line by line until end marker
            String line;
            while ((line = reducerIn.readLine()) != null) {
                if (line.equals("END_FINAL_RESULTS")) {
                    break; // End marker received
                }
                if (line.equals("ERROR_RESULTS_FAILED")) {
                    System.err.println("Master (Job " + jobId + "): Reducer reported failure retrieving results.");
                    finalResults.clear(); // Discard any partial results
                    break; // Stop reading on error
                }
                if (line.equals("ERROR_MISSING_JOB_ID")) { // Handle specific error from Reducer
                    System.err.println("Master (Job " + jobId + "): Reducer reported missing job ID error.");
                    finalResults.clear();
                    break;
                }
                // Add valid result line to the list
                if (!line.trim().isEmpty()) { // Avoid adding empty lines if any
                    finalResults.add(line);
                }
            }
            // Check if loop exited because stream ended unexpectedly
            if (line == null) {
                System.err.println("Master (Job " + jobId + "): Reducer disconnected unexpectedly while sending results.");
                finalResults.clear(); // Results are incomplete/unreliable
            }
        } catch (IOException e) {
            System.err.println("Master (Job " + jobId + "): Failed to communicate with Reducer (GET_FINAL_RESULTS): " + e.getMessage());
            finalResults.clear(); // Clear results on communication error
        }

        System.out.println("Master: MapReduce Filter (Job: " + jobId + ") completed. Final Results Count: " + finalResults.size());
        return finalResults;
    }


    /**
     * Handles a purchase request from a client.
     * Forwards the request to the appropriate worker and reads the response.
     *
     * @param storeName   The name of the store.
     * @param productName The name of the product.
     * @param quantity    The quantity to purchase.
     * @return A PurchaseResponse object received from the worker, or null if an error occurs.
     */
    public PurchaseResponse performPurchase(String storeName, String productName, int quantity) {
        int workerId = getWorkerIdForStore(storeName);
        WorkerConnection connection = workers.get(workerId);

        if (connection != null) {
            try {
                System.out.println("Master: Forwarding purchase request for " + quantity + "x '" + productName + "' from '" + storeName + "' to Worker " + workerId);
                // Create the Master -> Worker purchase request object
                PurchaseRequest purchaseRequestObject = new PurchaseRequest(storeName, productName, quantity);
                connection.sendRequest(purchaseRequestObject); // Send request

                // Wait for and read the response object from the worker
                Object responseObj = connection.readResponse();

                // Check if the response is of the expected type
                if (responseObj instanceof PurchaseResponse) {
                    PurchaseResponse purchaseResponse = (PurchaseResponse) responseObj;
                    System.out.println("Master: Received PurchaseResponse from Worker " + workerId + ": Status=" + purchaseResponse.getStatus());
                    return purchaseResponse; // Return the valid response object
                } else {
                    // Log error if unexpected response type received
                    System.err.println("Master: Received unexpected response type from Worker " + workerId + " for purchase: " + (responseObj != null ? responseObj.getClass().getName() : "null"));
                    // Return a custom error response or null
                    return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Unexpected response type from worker");
                }
            } catch (IOException e) {
                System.err.println("Master: Communication error during purchase with Worker " + workerId + ": " + e.getMessage());
                return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Communication error with worker");
            } catch (ClassNotFoundException e) {
                System.err.println("Master: Could not find class for PurchaseResponse from Worker " + workerId + ": " + e.getMessage());
                return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Deserialization error from worker");
            } catch (Exception e) { // Catch other unexpected errors
                System.err.println("Master: Unexpected error during purchase processing for Worker " + workerId + ": " + e.getMessage());
                return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Unexpected error during purchase");
            }
        } else {
            // Worker connection not found
            System.err.println("Master: Worker not found for store '" + storeName + "' (ID: " + workerId + ") during purchase.");
            // Return a specific error status if possible, or null/generic error
            return new PurchaseResponse(PurchaseResponse.Status.FAIL_STORE_NOT_FOUND, "Worker for store not found"); // Or FAIL_WORKER_ERROR
        }
    }


    /**
     * Gracefully shuts down connections to all workers.
     * Called during Master shutdown.
     */
    public void shutdownWorkers() {
        System.out.println("Master: Terminating connections to Workers...");
        // Iterate through a copy of the values to avoid potential ConcurrentModificationException
        // if a connection fails and needs to be removed elsewhere (though less likely here)
        List<WorkerConnection> connectionsToClose = new ArrayList<>(workers.values());
        for (WorkerConnection connection : connectionsToClose) {
            connection.close(); // Close each connection
        }
        workers.clear(); // Clear the map
        sortedWorkerIds = new ArrayList<>(); // Clear the sorted list
        System.out.println("Master: All connections to Workers terminated and cleared.");
    }
}
