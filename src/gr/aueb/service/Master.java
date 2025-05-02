package gr.aueb.service;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import gr.aueb.client.ClientRequestHandler;
import gr.aueb.client.dtos.FilterStoresClientRequest;
import gr.aueb.client.dtos.PurchaseRequest;
import gr.aueb.client.dtos.PurchaseResponse;
import gr.aueb.dtos.BaseRequest;
import gr.aueb.model.FilterCriteria;
import gr.aueb.manager.ManagerRequestHandler;

/**
 * Master Node for the Distributed Food Delivery System.
 * Receives configuration and established WorkerConnections via its constructor or setters.
 * Uses Object Streams for Master <-> Worker communication.
 * Listens for client connections, identifies client type (Client/Manager),
 * and starts the appropriate handler (RegularClientHandler or ManagerClientHandler).
 * Designed to be run as an object within a larger application/launcher.
 */
public class Master implements Runnable { // Implement Runnable to be started in a thread

    // Stores active connections to workers (Worker ID -> WorkerConnection)
    // This will be populated by the launcher
    private final Map<Integer, WorkerConnection> workers = new ConcurrentHashMap<>();
    // Store worker IDs in a sorted list for consistent hashing
    private List<Integer> sortedWorkerIds = new ArrayList<>();

    // Configuration details (to be set by the launcher)
    private String reducerHost;
    private int reducerPort;
    private int masterListenPort; // Port for Master to listen on

    /**
     * Manages the TCP connection and communication with a single Worker node
     * using Object Streams. (Inner class remains the same)
     */
    public static class WorkerConnection implements Closeable { // Made static for potential external use if needed
        private final Socket socket;
        private ObjectOutputStream objectOut;
        private ObjectInputStream objectIn;
        private final int workerId;
        private final String ip;
        private final int port;
        private final Object outputStreamLock = new Object();

        public WorkerConnection(Socket socket, int workerId, String ip, int port) throws IOException {
            this.socket = socket;
            this.workerId = workerId;
            this.ip = ip;
            this.port = port;
            this.objectOut = new ObjectOutputStream(socket.getOutputStream());
            this.objectIn = new ObjectInputStream(socket.getInputStream());
            // Log moved to where connection is established (Launcher or previous Master)
        }

        public void sendRequest(BaseRequest request) throws IOException {
            synchronized (outputStreamLock) {
                System.out.println("Master -> Worker " + workerId + ": Sending " + request.getClass().getSimpleName() + "...");
                objectOut.writeObject(request);
                objectOut.flush();
                objectOut.reset();
            }
        }

        public Object readResponse() throws IOException, ClassNotFoundException {
            return objectIn.readObject();
        }

        @Override
        public void close() {
            try {
                if (objectOut != null) objectOut.close();
            } catch (IOException e) {
            }
            try {
                if (objectIn != null) objectIn.close();
            } catch (IOException e) {
            }
            try {
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (IOException e) {
                System.err.println("Master: Error closing connection to Worker " + workerId + ": " + e.getMessage());
            } finally {
                System.out.println("Master: WorkerConnection closed for Worker " + workerId + " @ " + ip + ":" + port);
            }
        }

        public int getWorkerId() {
            return workerId;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }
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

        // Populate and sort worker IDs
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
     * Reads an initial identifier line to determine client type and starts
     * the appropriate handler (RegularClientHandler or ManagerClientHandler).
     */
    public void startServer(int port) { // Made public if called externally
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Master: Listening for client connections on port " + port + "...");
            while (!Thread.currentThread().isInterrupted()) { // Check for interruption
                Socket clientSocket = null;
                ObjectOutputStream objOut = null;
                ObjectInputStream objIn = null;
                try {
                    clientSocket = serverSocket.accept(); // Blocks here
                    System.out.println("gr.aueb.service.Master: New connection from " + clientSocket.getInetAddress().getHostAddress());

                    // --- Identify Client Type ---
                    clientSocket.setSoTimeout(5000);
                    BufferedReader initialReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String clientType = initialReader.readLine();
                    clientSocket.setSoTimeout(0);

                    Runnable handler = null;

                    if (clientType != null) {
                        objOut = new ObjectOutputStream(clientSocket.getOutputStream());
                        objIn = new ObjectInputStream(clientSocket.getInputStream());

                        if ("CLIENT".equals(clientType.trim().toUpperCase())) {
                            System.out.println("gr.aueb.service.Master: Identified connection as REGULAR CLIENT.");
                            handler = new ClientRequestHandler(clientSocket, this, objIn, objOut);
                        } else if ("MANAGER".equals(clientType.trim().toUpperCase())) {
                            System.out.println("gr.aueb.service.Master: Identified connection as MANAGER CLIENT.");
                            handler = new ManagerRequestHandler(clientSocket, this, objIn, objOut);
                        } else {
                            System.err.println("gr.aueb.service.Master: Received unknown client type identifier: '" + clientType + "' from " + clientSocket.getInetAddress().getHostAddress());
                        }
                    } else {
                        System.err.println("gr.aueb.service.Master: Did not receive client type identifier from " + clientSocket.getInetAddress().getHostAddress() + " (timeout or disconnect).");
                    }

                    if (handler != null) {
                        new Thread(handler).start(); // Start handler in its own thread
                    } else {
                        System.err.println("gr.aueb.service.Master: Closing unidentified connection from " + clientSocket.getInetAddress().getHostAddress());
                        if (objOut != null) try {
                            objOut.close();
                        } catch (IOException ioex) {
                        }
                        if (objIn != null) try {
                            objIn.close();
                        } catch (IOException ioex) {
                        }
                        if (clientSocket != null) try {
                            clientSocket.close();
                        } catch (IOException ioex) {
                        }
                    }

                } catch (java.net.SocketTimeoutException e) {
                    System.err.println("gr.aueb.service.Master: Timeout waiting for client type identifier from " + (clientSocket != null ? clientSocket.getInetAddress().getHostAddress() : "unknown") + ". Closing connection.");
                    if (clientSocket != null) try {
                        clientSocket.close();
                    } catch (IOException ioex) { /* ignore */ }
                } catch (SocketException e) {
                    if ("Socket closed".equals(e.getMessage())) {
                        System.out.println("gr.aueb.service.Master: Server socket closed, stopping listening.");
                        break; // Exit loop if server socket is closed
                    } else {
                        System.err.println("gr.aueb.service.Master: SocketException while accepting connections: " + e.getMessage());
                    }
                } catch (IOException e) {
                    System.err.println("gr.aueb.service.Master: Error during client connection setup or identification: " + e.getMessage());
                    if (objOut != null) try {
                        objOut.close();
                    } catch (IOException ioex) {
                    }
                    if (objIn != null) try {
                        objIn.close();
                    } catch (IOException ioex) {
                    }
                    if (clientSocket != null) try {
                        clientSocket.close();
                    } catch (IOException ioex) { /* ignore */ }
                }
            }
        } catch (IOException e) {
            System.err.println("gr.aueb.service.Master: Could not start server on port " + port + ": " + e.getMessage());
            // Don't call shutdownWorkers here, let the launcher handle it
        } finally {
            System.out.println("gr.aueb.service.Master: Server loop finished.");
            // Ensure workers are closed when gr.aueb.service.Master stops listening
            shutdownWorkers();
        }
    }

    /**
     * Selects the appropriate worker ID for a given store name
     */
    public int getWorkerIdForStore(String storeName) {
        if (sortedWorkerIds.isEmpty()) {
            throw new IllegalStateException("No workers connected.");
        }
        int workerIndex = Math.abs(storeName.hashCode()) % sortedWorkerIds.size();
        return sortedWorkerIds.get(workerIndex);
    }

    /**
     * Extracts the store name from a JSON string
     */
    public String extractStoreNameFromJson(String jsonContent) {
        try {
            String searchKey = "\"StoreName\"";
            int keyIndex = jsonContent.indexOf(searchKey);
            if (keyIndex == -1) return null;
            int colonIndex = jsonContent.indexOf(':', keyIndex + searchKey.length());
            if (colonIndex == -1) return null;
            int valueStartIndex = jsonContent.indexOf('"', colonIndex + 1);
            if (valueStartIndex == -1) return null;
            int valueEndIndex = jsonContent.indexOf('"', valueStartIndex + 1);
            if (valueEndIndex == -1) return null;
            return jsonContent.substring(valueStartIndex + 1, valueEndIndex);
        } catch (Exception e) {
            System.err.println("gr.aueb.service.Master: Error extracting StoreName from JSON: " + e.getMessage());
            return null;
        }
    }

    /**
     * Forwards a Serializable request object to a specific worker
     */
    public boolean forwardRequestObjectToWorker(int targetWorkerId, BaseRequest request) {
        WorkerConnection connection = workers.get(targetWorkerId);
        if (connection != null) {
            try {
                connection.sendRequest(request);
                return true;
            } catch (Exception e) {
                System.err.println("gr.aueb.service.Master: Error sending request object to gr.aueb.service.Worker " + targetWorkerId + ": " + e.getMessage());
                return false;
            }
        } else {
            System.err.println("gr.aueb.service.Master: gr.aueb.service.Worker with ID " + targetWorkerId + " not found for forwarding request object.");
            return false;
        }
    }

    /**
     * Forwards a text-based request (list of strings) to a specific worker (TEMPORARY - Should be removed)
     */
    public boolean forwardRequestToWorker(int targetWorkerId, List<String> requestLines) {
        System.err.println("gr.aueb.service.Master: forwardRequestToWorker (text) called but only object sending is supported by WorkerConnection.");
        return false;
    }


    /**
     * Orchestrates the MapReduce process for filtering stores via the gr.aueb.service.Reducer
     */
    public List<String> performMapReduceFilterViaReducer(FilterCriteria filterCriteria) {
        String jobId = "job-" + UUID.randomUUID().toString();
        int numWorkers = workers.size();
        List<String> finalResults = new ArrayList<>();
        if (filterCriteria == null) {
            System.err.println("gr.aueb.service.Master: Null filter criteria received for MapReduce (Job: " + jobId + ")");
            return finalResults;
        }
        if (numWorkers == 0) {
            System.err.println("gr.aueb.service.Master: No connected workers to perform MapReduce (Job: " + jobId + ")");
            return finalResults;
        }
        System.out.println("gr.aueb.service.Master: Starting MapReduce Filter (Job: " + jobId + ") via gr.aueb.service.Reducer (" + reducerHost + ":" + reducerPort + ")");
        try (Socket reducerSocket = new Socket(reducerHost, reducerPort); PrintWriter reducerOut = new PrintWriter(reducerSocket.getOutputStream(), true)) {
            System.out.println("gr.aueb.service.Master: Notifying gr.aueb.service.Reducer to start Job ID: " + jobId);
            reducerOut.println("START_REDUCE " + jobId + " " + numWorkers);
        } catch (IOException e) {
            System.err.println("gr.aueb.service.Master: Failed to communicate with gr.aueb.service.Reducer (START_REDUCE) for Job ID " + jobId + ": " + e.getMessage());
            return finalResults;
        }
        System.out.println("gr.aueb.service.Master: Sending Map tasks to Workers for Job ID: " + jobId);
        FilterStoresClientRequest mapRequestObject = new FilterStoresClientRequest(filterCriteria);
        List<Thread> workerThreads = new ArrayList<>();
        for (WorkerConnection worker : workers.values()) {
            final WorkerConnection targetWorker = worker;
            Thread t = new Thread(() -> {
                try {
                    System.out.println("gr.aueb.service.Master: Sending FilterRequest (Job " + jobId + ") to gr.aueb.service.Worker " + targetWorker.getWorkerId());
                    targetWorker.sendRequest(mapRequestObject);
                } catch (Exception e) {
                    System.err.println("gr.aueb.service.Master: Communication error (Map) with gr.aueb.service.Worker " + targetWorker.getWorkerId() + " for Job ID " + jobId + ": " + e.getMessage());
                }
            });
            workerThreads.add(t);
            t.start();
        }
        System.out.println("gr.aueb.service.Master: Requesting final results from gr.aueb.service.Reducer for Job ID: " + jobId);
        try (Socket reducerSocket = new Socket(reducerHost, reducerPort); PrintWriter reducerOut = new PrintWriter(reducerSocket.getOutputStream(), true); BufferedReader reducerIn = new BufferedReader(new InputStreamReader(reducerSocket.getInputStream()))) {
            reducerOut.println("GET_FINAL_RESULTS " + jobId);
            String line;
            while ((line = reducerIn.readLine()) != null && !line.equals("END_FINAL_RESULTS")) {
                if (line.equals("ERROR_RESULTS_FAILED")) {
                    System.err.println("gr.aueb.service.Master: gr.aueb.service.Reducer reported failure for Job ID: " + jobId);
                    finalResults.clear();
                    break;
                }
                if (!line.trim().isEmpty()) {
                    finalResults.add(line);
                }
            }
            if (line == null) {
                System.err.println("gr.aueb.service.Master: gr.aueb.service.Reducer disconnected unexpectedly while sending results for Job ID: " + jobId);
                finalResults.clear();
            }
        } catch (IOException e) {
            System.err.println("gr.aueb.service.Master: Failed to communicate with gr.aueb.service.Reducer (GET_FINAL_RESULTS) for Job ID " + jobId + ": " + e.getMessage());
            finalResults.clear();
        }
        System.out.println("gr.aueb.service.Master: MapReduce Filter (Job: " + jobId + ") completed. Final Results: " + finalResults.size());
        return finalResults;
    }


    /**
     * Handles a purchase request
     */
    public String performPurchase(String storeName, String productName, int quantity) {
        int workerId = getWorkerIdForStore(storeName);
        WorkerConnection connection = workers.get(workerId);
        if (connection != null) {
            try {
                System.out.println("gr.aueb.service.Master: Forwarding purchase request for '" + productName + "' from '" + storeName + "' to gr.aueb.service.Worker " + workerId);
                PurchaseRequest purchaseRequestObject = new PurchaseRequest(storeName, productName, quantity);
                connection.sendRequest(purchaseRequestObject);
                Object responseObj = connection.readResponse();
                if (responseObj instanceof PurchaseResponse) {
                    PurchaseResponse purchaseResponse = (PurchaseResponse) responseObj;
                    System.out.println("gr.aueb.service.Master: Purchase response from gr.aueb.service.Worker " + workerId + ": " + purchaseResponse.getStatus());
                    return purchaseResponse.toStatusCodeString();
                } else {
                    System.err.println("gr.aueb.service.Master: Received unexpected response type from gr.aueb.service.Worker " + workerId + " for purchase: " + (responseObj != null ? responseObj.getClass().getName() : "null"));
                    return "ERROR_UNEXPECTED_RESPONSE";
                }
            } catch (IOException e) {
                System.err.println("gr.aueb.service.Master: Communication error during purchase with gr.aueb.service.Worker " + workerId + ": " + e.getMessage());
                return "ERROR_WORKER_COMMUNICATION";
            } catch (ClassNotFoundException e) {
                System.err.println("gr.aueb.service.Master: Could not find class for response from gr.aueb.service.Worker " + workerId + ": " + e.getMessage());
                return "ERROR_DESERIALIZATION";
            }
        } else {
            System.err.println("gr.aueb.service.Master: gr.aueb.service.Worker not found for store '" + storeName + "' (ID: " + workerId + ")");
            return "ERROR_WORKER_NOT_FOUND";
        }
    }


    /**
     * Gracefully shuts down connections to all workers.
     */
    public void shutdownWorkers() { // Made public if launcher needs to call it
        System.out.println("gr.aueb.service.Master: Terminating connections to Workers...");
        for (WorkerConnection connection : workers.values()) {
            connection.close();
        }
        workers.clear();
        sortedWorkerIds = new ArrayList<>();
        System.out.println("gr.aueb.service.Master: All connections to Workers terminated.");
    }
}
