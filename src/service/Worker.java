package service;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService; // Import if using ExecutorService
import java.util.concurrent.Executors; // Import if using ExecutorService

// Import necessary DTOs and Data classes
import dtos.requests.*;
import dtos.responses.*;
import dtos.reducer.*;
import manager.dtos.UpdateProductRequest;
import model.*; // Import the model package

/**
 * Worker Node for the Distributed Food Delivery System.
 * Receives Reducer address via command-line arguments.
 * Uses Object Streams for Master <-> Worker and Worker -> Reducer communication.
 * Uses synchronized HashMap for storing data.
 * Handles Master connections directly without an inner handler class.
 */
public class Worker { // implements Runnable if started in thread by launcher

    private final int port;
    private final String reducerHost; // Store Reducer host
    private final int reducerPort;    // Store Reducer port
    // Use HashMap + explicit synchronization
    private final Map<String, Store> stores = new HashMap<>();
    private final Object storesLock = new Object(); // Lock for accessing stores map
    private volatile boolean isRunning = true; // Flag to control the main loop
    private ServerSocket serverSocket; // Keep reference to close it
    // Optional: Use a thread pool to manage connection handling threads
    private final ExecutorService connectionExecutor = Executors.newCachedThreadPool();

    public Worker(int port, String reducerHost, int reducerPort) {
        this.port = port;
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;
        if (reducerHost == null || reducerHost.trim().isEmpty() || reducerPort <= 0) {
            throw new IllegalArgumentException("Invalid Reducer configuration provided to Worker.");
        }
        System.out.println("Worker configured for port " + port + ", Reducer at " + reducerHost + ":" + reducerPort);
    }

    // Method to be called by the launcher thread
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Worker: Listening for Master connections on port " + port + "...");

            while (isRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    Socket masterSocket = serverSocket.accept(); // Blocks here
                    if (!isRunning) { // Check flag after accept returns
                        masterSocket.close();
                        break;
                    }
                    System.out.println("Worker: Accepted connection from Master: " + masterSocket.getInetAddress().getHostAddress());

                    // Submit the connection handling logic to the executor service
                    connectionExecutor.submit(() -> handleMasterConnection(masterSocket));

                } catch (SocketException e) {
                    if (!isRunning) {
                        System.out.println("Worker: Server socket closed, stopping acceptance.");
                    } else {
                        System.err.println("Worker: SocketException while accepting connection: " + e.getMessage());
                    }
                } catch (IOException e) {
                    if (isRunning) { // Avoid logging errors if we are shutting down
                        System.err.println("Worker: Error accepting connection from Master: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Worker: Could not start worker server on port " + port + ": " + e.getMessage());
        } finally {
            System.out.println("Worker on port " + port + " shutting down listener loop.");
            if (serverSocket != null && !serverSocket.isClosed()) {
                try { serverSocket.close(); }
                catch (IOException e) { System.err.println("Worker: Error closing server socket: " + e.getMessage()); }
            }
            // Shutdown the executor service when the worker stops
            shutdownConnectionExecutor();
        }
    }

    // Method to gracefully stop the worker server loop and connection executor
    public void stop() {
        System.out.println("Worker on port " + port + " received stop signal.");
        isRunning = false;
        // Close server socket to interrupt accept()
        if (serverSocket != null && !serverSocket.isClosed()) {
            try { serverSocket.close(); System.out.println("Worker: Closed server socket on port " + port); }
            catch (IOException e) { /* ignore */ }
        }
        // Shutdown the connection executor
        shutdownConnectionExecutor();
    }

    // Helper method to shut down the executor service
    private void shutdownConnectionExecutor() {
        System.out.println("Worker: Shutting down connection handler thread pool...");
        connectionExecutor.shutdown();
        try {
            if (!connectionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.err.println("Worker: Connection handler pool did not terminate gracefully, forcing shutdown...");
                connectionExecutor.shutdownNow();
                if (!connectionExecutor.awaitTermination(5, TimeUnit.SECONDS))
                    System.err.println("Worker: Connection handler pool did not terminate even after forcing.");
            }
        } catch (InterruptedException ie) {
            connectionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Worker: Connection handler pool shut down.");
    }

    /**
     * Handles the communication for a single Master connection.
     * This method contains the logic previously in MasterRequestHandler.run().
     * @param masterSocket The socket connected to the Master.
     */
    private void handleMasterConnection(Socket masterSocket) {
        String masterAddress = masterSocket.getInetAddress().getHostAddress();
        // Use try-with-resources for Object Streams
        try (
                ObjectOutputStream objOut = new ObjectOutputStream(masterSocket.getOutputStream());
                // Initialize Input Stream after Output Stream
                ObjectInputStream objIn = new ObjectInputStream(masterSocket.getInputStream())
        )
        {
            System.out.println("Handler: Starting request processing from Master " + masterAddress);
            final Object outputStreamLock = new Object(); // Lock specific to this connection's output

            Object requestObject;
            // Read request objects from the Master
            while (isRunning && (requestObject = objIn.readObject()) != null) {
                if (requestObject instanceof BaseRequest) {
                    System.out.println("Handler: Received request: " + requestObject.getClass().getSimpleName() + " from Master " + masterAddress);
                    processWorkerRequest((BaseRequest) requestObject, objOut, outputStreamLock); // Pass streams/lock
                } else {
                    System.err.println("Handler: Received unknown object type from Master: " + requestObject.getClass().getName());
                }
            }
            System.out.println("Handler: Master " + masterAddress + " stream closed or worker stopping.");

        } catch (EOFException e) {
            System.out.println("Handler: Master " + masterAddress + " closed the connection (EOF).");
        } catch (SocketException e) {
            if (isRunning) System.out.println("Handler: Master connection " + masterAddress + " closed or reset: " + e.getMessage());
            else System.out.println("Handler: Socket closed during shutdown for Master " + masterAddress);
        } catch (IOException | ClassNotFoundException e) {
            if (isRunning) System.err.println("Handler: Error communicating with Master " + masterAddress + ": " + e.getMessage());
        } catch (Exception e) { // Catch unexpected errors
            if (isRunning) System.err.println("Handler: Unexpected error processing request from Master " + masterAddress + ": " + e.getMessage());
        } finally {
            // Close the client socket for this specific connection
            try {
                if (masterSocket != null && !masterSocket.isClosed()) {
                    masterSocket.close();
                }
            } catch (IOException e) {
                System.err.println("Handler: Error closing Master socket (" + masterAddress + "): " + e.getMessage());
            }
            System.out.println("Handler: Stopped processing for Master " + masterAddress);
        }
    }

    /**
     * Processes different types of BaseRequest objects from Master.
     * This method contains the logic previously in MasterRequestHandler.processWorkerRequest().
     * @param request The received BaseRequest object.
     * @param objOut The ObjectOutputStream to send responses back to the Master.
     * @param outputStreamLock The lock object for synchronizing writes to objOut.
     */
    private void processWorkerRequest(BaseRequest request, ObjectOutputStream objOut, Object outputStreamLock) throws IOException {
        if (request instanceof StoreDataRequest) {
            // Call worker's method
            storeData(((StoreDataRequest) request).getStore());
            // No response needed back to Master for this action currently
        } else if (request instanceof UpdateProductRequest) {
            // Call worker's method
            updateStoreProduct((UpdateProductRequest) request);
            // No response needed back to Master for this action currently
        } else if (request instanceof FilterRequest) {
            // Handle filtering and send results/done to Reducer
            handleProcessFilterAndSendToReducer((FilterRequest) request);
            // No response needed back to Master for this action
        } else if (request instanceof PurchaseRequest) {
            // Call worker's method to get response object
            PurchaseResponse response = processPurchase((PurchaseRequest) request);
            // Send response object back to Master
            sendResponse(response, objOut, outputStreamLock);
        } else {
            System.err.println("Handler: Unknown BaseRequest type: " + request.getClass().getName());
        }
    }

    /**
     * Handles the FilterRequest logic: performs filtering and sends results/done
     * signal directly to the Reducer using Object Streams.
     * This method contains the logic previously in MasterRequestHandler.handleProcessFilterAndSendToReducer().
     * @param request The FilterRequest object.
     */
    private void handleProcessFilterAndSendToReducer(FilterRequest request) {
        String jobId = request.getJobId();
        FilterCriteria criteria = request.getCriteria();
        // Use reducerHost and reducerPort stored in the Worker instance
        System.out.println("Handler: Received PROCESS_FILTER for Job ID: " + jobId + ", using Reducer: " + reducerHost + ":" + reducerPort);

        List<String> results = processFilterLogic(criteria); // Call worker's logic method

        System.out.println("Handler: Sending " + results.size() + " results to Reducer for Job ID: " + jobId);
        ObjectOutputStream reducerObjectOut = null;
        try (Socket reducerSocket = new Socket(this.reducerHost, this.reducerPort)) { // Use worker's fields
            reducerObjectOut = new ObjectOutputStream(reducerSocket.getOutputStream());
            WorkerResultsMessage resultsMessage = new WorkerResultsMessage(jobId, results);
            System.out.println("Handler -> Reducer: Sending WorkerResultsMessage for Job ID: " + jobId);
            reducerObjectOut.writeObject(resultsMessage); reducerObjectOut.flush(); reducerObjectOut.reset();
            WorkerDoneMessage doneMessage = new WorkerDoneMessage(jobId);
            System.out.println("Handler -> Reducer: Sending WorkerDoneMessage for Job ID: " + jobId);
            reducerObjectOut.writeObject(doneMessage); reducerObjectOut.flush(); reducerObjectOut.reset();
            System.out.println("Handler: Finished sending objects to Reducer for Job ID: " + jobId);
        } catch (UnknownHostException e) { System.err.println("Handler (PROCESS_FILTER): Reducer host not found: " + this.reducerHost + " for Job ID " + jobId);
        } catch (IOException e) { System.err.println("Handler (PROCESS_FILTER): Error communicating with Reducer (" + this.reducerHost + ":" + this.reducerPort + ") for Job ID " + jobId + ": " + e.getMessage()); }
        finally { if (reducerObjectOut != null) { try { reducerObjectOut.close(); } catch (IOException e) {} } }
    }

    /**
     * Sends a Serializable response object back to the specific Master connection.
     * This method contains the logic previously in MasterRequestHandler.sendResponse().
     * @param response The Serializable object to send.
     * @param objOut The ObjectOutputStream for the specific Master connection.
     * @param outputStreamLock The lock for the specific Master connection's output stream.
     */
    private void sendResponse(Serializable response, ObjectOutputStream objOut, Object outputStreamLock) {
        try {
            synchronized (outputStreamLock) {
                // Cannot easily get remote address here without passing socket, log generically
                System.out.println("Handler -> Master: Sending " + response.getClass().getSimpleName() + "...");
                objOut.writeObject(response);
                objOut.flush();
                objOut.reset();
            }
        } catch (IOException e) {
            System.err.println("Handler: Error sending response to Master: " + e.getMessage());
            // Closing the connection should be handled by handleMasterConnection's finally block
        }
    }


    // storeData, updateStoreProduct, processFilterLogic, processPurchase, calculateDistance
    // methods remain the same logic as before.
    public boolean storeData(Store store) { /* ... same ... */ if (store == null || store.storeName == null || store.storeName.trim().isEmpty()) { System.err.println("Worker: Received invalid Store data (null or missing name)."); return false; } try { store.calculateAndSetPriceCategory(); synchronized (storesLock) { stores.put(store.storeName, store); } System.out.println("Worker: Stored/Updated store: " + store.storeName + " (" + store.products.size() + " products)"); return true; } catch (Exception e) { System.err.println("Worker: Error processing STORE_DATA for " + store.storeName + ": " + e.getMessage()); return false; } }
    public boolean updateStoreProduct(UpdateProductRequest request) { /* ... same ... */ if (request == null || request.getStoreName() == null) { System.err.println("Worker (updateStoreProduct): Received invalid request."); return false; } String storeName = request.getStoreName(); Store store; synchronized(storesLock) { store = stores.get(storeName); } if (store == null) { System.err.println("Worker (updateStoreProduct): Store not found: " + storeName); return false; } String productName = request.getProductName(); try { switch (request.getAction()) { case ADD: if (productName == null || request.getProductType() == null) return false; Product np = new Product(productName, request.getProductType(), request.getAmount(), request.getPrice()); store.addProduct(np); System.out.println("Worker: Added product '" + productName + "' to store '" + storeName + "'"); return true; case REMOVE: if (productName == null) return false; Product rem = store.removeProduct(productName); if (rem != null) { System.out.println("Worker: Removed product '" + productName + "' from store '" + storeName + "'"); return true; } else { System.err.println("Worker (REMOVE): Product '" + productName + "' not found in store '" + storeName + "'"); return false; } case UPDATE_STOCK: if (productName == null) return false; Product pu = store.getProduct(productName); if (pu != null) { pu.setStock(request.getAmount()); System.out.println("Worker: Updated stock for '" + productName + "' in store '" + storeName + "' to " + request.getAmount()); return true; } else { System.err.println("Worker (UPDATE_STOCK): Product '" + productName + "' not found in store '" + storeName + "'"); return false; } default: System.err.println("Worker: Unknown action for UPDATE_STORE_PRODUCT: " + request.getAction()); return false; } } catch (Exception e) { System.err.println("Worker (updateStoreProduct): Unexpected error for store " + storeName + ": " + e.getMessage()); return false; } }
    public List<String> processFilterLogic(FilterCriteria criteria) { /* ... same ... */ List<String> results = new ArrayList<>(); if (criteria == null) { System.err.println("Worker (processFilterLogic): Received null FilterCriteria."); return results; } System.out.println("Worker: Processing filter - Lat:" + criteria.getClientLatitude() + ", Lon:" + criteria.getClientLongitude() + ", Dist:" + criteria.getMaxDistance() + ", FoodCats:" + criteria.getFoodCategories() + ", Stars:" + criteria.getMinStars() + ", PriceCats:" + criteria.getPriceCategories()); List<Store> storesToCheck; synchronized (storesLock) { storesToCheck = new ArrayList<>(stores.values()); } try { for (Store store : storesToCheck) { double distance = calculateDistance(criteria.getClientLatitude(), criteria.getClientLongitude(), store.latitude, store.longitude); if (distance > criteria.getMaxDistance()) continue; List<String> targetFoodCategories = criteria.getFoodCategories(); if (!targetFoodCategories.isEmpty() && !targetFoodCategories.contains(store.foodCategory)) continue; if (store.stars < criteria.getMinStars()) continue; List<String> targetPriceCategories = criteria.getPriceCategories(); if (!targetPriceCategories.isEmpty() && !targetPriceCategories.contains(store.priceCategory)) continue; results.add(store.toString()); System.out.println("Worker: Filter match found: " + store.storeName); } } catch (Exception e) { System.err.println("Worker (processFilterLogic): Unexpected error during filtering: " + e.getMessage()); return new ArrayList<>(); } System.out.println("Worker: Filter processing completed. Found: " + results.size()); return results; }
    public PurchaseResponse processPurchase(PurchaseRequest request) { /* ... same ... */ if (request == null || request.getStoreName() == null || request.getProductName() == null) { return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Invalid purchase request object."); } String storeName = request.getStoreName(); String productName = request.getProductName(); int quantity = request.getQuantity(); if (quantity <= 0) { return new PurchaseResponse(PurchaseResponse.Status.FAIL_INVALID_QTY); } Store store; synchronized(storesLock){ store = stores.get(storeName); } if (store == null) { System.err.println("Worker (Purchase): Store not found: " + storeName); return new PurchaseResponse(PurchaseResponse.Status.FAIL_STORE_NOT_FOUND); } Product product = store.getProduct(productName); if (product == null) { System.err.println("Worker (Purchase): Product '" + productName + "' not found in store '" + storeName + "'"); return new PurchaseResponse(PurchaseResponse.Status.FAIL_PRODUCT_NOT_FOUND); } boolean success = product.decreaseStock(quantity); if (success) { System.out.println("Worker: Purchase successful " + quantity + " x '" + productName + "' from '" + storeName + "'. New stock: " + product.getAvailableAmount()); return new PurchaseResponse(PurchaseResponse.Status.OK); } else { System.out.println("Worker: Purchase failed " + quantity + " x '" + productName + "' from '" + storeName + "' - Insufficient stock (" + product.getAvailableAmount() + ")"); return new PurchaseResponse(PurchaseResponse.Status.FAIL_STOCK); } }
    private static double calculateDistance(double lat1, double lon1, double lat2, double lon2) { /* ... same ... */ final int R = 6371; double latDistance = Math.toRadians(lat2 - lat1); double lonDistance = Math.toRadians(lon2 - lon1); double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2); double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)); return R * c; }


    public static void main(String[] args) { /* ... same as before ... */ if (args.length < 3) { System.err.println("Usage: java Worker <port> <reducerHost> <reducerPort>"); System.exit(1); } int port; String reducerHost; int reducerPort; try { port = Integer.parseInt(args[0]); reducerHost = args[1]; reducerPort = Integer.parseInt(args[2]); } catch (NumberFormatException e) { System.err.println("Invalid Port format in arguments."); System.exit(1); return; } catch (ArrayIndexOutOfBoundsException e) { System.err.println("Missing required arguments: <port> <reducerHost> <reducerPort>"); System.exit(1); return; } Worker worker = new Worker(port, reducerHost, reducerPort); Runtime.getRuntime().addShutdownHook(new Thread(() -> { System.out.println("Worker shutdown hook activated for port " + port); worker.stop(); })); worker.start(); }

    // MasterRequestHandler inner class has been removed.

}
