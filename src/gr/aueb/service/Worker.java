package gr.aueb.service;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;

import gr.aueb.client.dtos.*;
import gr.aueb.dtos.*; // BaseRequest, StoreDataRequest, FilterStoresClientRequest (Master->Worker)
import gr.aueb.manager.dtos.StoreDataRequest;
import gr.aueb.manager.dtos.UpdateProductRequest; // Master -> Worker
import gr.aueb.model.*;

/**
 * Worker Node for the Distributed Food Delivery System.
 * Implements Runnable to be run in a thread.
 * Receives Reducer address via constructor.
 * Uses Object Streams for Master <-> Worker and Worker -> Reducer communication.
 * Uses synchronized HashMap for storing data.
 * Handles Master connections using manually created threads.
 * Avoids java.util.concurrent package and WorkerDoneMessage.
 */
public class Worker implements Runnable {

    private final int port;
    private final String reducerHost;
    private final int reducerPort;
    private final Map<String, Store> stores = new HashMap<>();
    private final Object storesLock = new Object();
    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;
    private final List<Thread> connectionThreads = Collections.synchronizedList(new ArrayList<>());


    public Worker(int port, String reducerHost, int reducerPort) {
        this.port = port;
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;
        if (reducerHost == null || reducerHost.trim().isEmpty() || reducerPort <= 0) {
            throw new IllegalArgumentException("Invalid Reducer configuration provided to Worker.");
        }
        System.out.println("Worker (Port " + port + "): Configured. Reducer at " + reducerHost + ":" + reducerPort); // LOG: Configured
    }

    /**
     * Main execution logic for the Worker thread.
     */
    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Worker (Port " + port + "): Listening for Master connections..."); // LOG: Start listening

            while (isRunning && !Thread.currentThread().isInterrupted()) {
                Socket masterSocket = null;
                try {
                    masterSocket = serverSocket.accept();
                    if (!isRunning) {
                        if (masterSocket != null) masterSocket.close();
                        break;
                    }
                    // LOG: Accepted connection
                    System.out.println("Worker (Port " + port + "): Accepted connection from Master: " + masterSocket.getInetAddress().getHostAddress());

                    Socket finalMasterSocket = masterSocket;
                    Thread handlerThread = new Thread(() -> {
                        try {
                            handleMasterConnection(finalMasterSocket);
                        } finally {
                            connectionThreads.remove(Thread.currentThread());
                            // LOG: Handler thread finished
                            System.out.println("Worker (Port " + port + "): Handler thread " + Thread.currentThread().getName() + " finished.");
                        }
                    }, "WorkerHandler-" + masterSocket.getRemoteSocketAddress());
                    connectionThreads.add(handlerThread);
                    handlerThread.start();

                } catch (SocketException e) {
                    if (!isRunning)
                        System.out.println("Worker (Port " + port + "): Server socket closed, stopping acceptance.");
                    else
                        System.err.println("Worker (Port " + port + "): SocketException accepting connection: " + e.getMessage());
                } catch (IOException e) {
                    if (isRunning)
                        System.err.println("Worker (Port " + port + "): Error accepting connection: " + e.getMessage());
                    // Close the specific socket if accept failed partially
                    if (masterSocket != null && !masterSocket.isClosed()) {
                        try {
                            masterSocket.close();
                        } catch (IOException ioEx) {
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Worker (Port " + port + "): Could not start server: " + e.getMessage());
        } finally {
            System.out.println("Worker (Port " + port + "): Shutting down listener loop."); // LOG: Shutdown listener
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) { /* ignore */ }
            }
            // Minimal wait for handlers
            synchronized (connectionThreads) {
                if (!connectionThreads.isEmpty()) {
                    System.out.println("Worker (Port " + port + "): Waiting briefly for " + connectionThreads.size() + " handler threads..."); // LOG: Waiting for handlers
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            System.out.println("Worker (Port " + port + "): Finished run method."); // LOG: Run method finished
        }
    }

    /**
     * Stops the Worker gracefully.
     */
    public void stop() {
        System.out.println("Worker (Port " + port + "): Received stop signal."); // LOG: Stop signal
        isRunning = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
                System.out.println("Worker (Port " + port + "): Closed server socket."); // LOG: Server socket closed
            } catch (IOException e) { /* ignore */ }
        }
        // Interrupt active handlers
        synchronized (connectionThreads) {
            if (!connectionThreads.isEmpty()) {
                System.out.println("Worker (Port " + port + "): Interrupting " + connectionThreads.size() + " active handler threads..."); // LOG: Interrupting handlers
                List<Thread> threadsToInterrupt = new ArrayList<>(connectionThreads);
                for (Thread t : threadsToInterrupt) {
                    if (t.isAlive()) {
                        t.interrupt();
                    }
                }
                connectionThreads.clear(); // Clear immediately after interrupting
            }
        }
    }


    /**
     * Handles communication for a single Master connection.
     *
     * @param masterSocket The socket connected to the Master.
     */
    private void handleMasterConnection(Socket masterSocket) {
        String masterAddress = masterSocket.getInetAddress().getHostAddress();
        String threadName = Thread.currentThread().getName(); // Get thread name for logging
        // Use try-with-resources for automatic stream closing
        try (
                ObjectOutputStream objOut = new ObjectOutputStream(masterSocket.getOutputStream());
                ObjectInputStream objIn = new ObjectInputStream(masterSocket.getInputStream())
        ) {
            // LOG: Start handling connection
            System.out.println("Handler (" + threadName + "): Starting processing for Master " + masterAddress);
            final Object outputStreamLock = new Object(); // Lock specific to this connection's output

            Object requestObject;
            while (isRunning && !Thread.currentThread().isInterrupted() && (requestObject = objIn.readObject()) != null) {
                if (requestObject instanceof BaseRequest) {
                    // LOG: Received request
                    System.out.println("Handler (" + threadName + "): Received request: " + requestObject.getClass().getSimpleName());
                    processWorkerRequest((BaseRequest) requestObject, objOut, outputStreamLock);
                } else {
                    System.err.println("Handler (" + threadName + "): Received unknown object type from Master: " + requestObject.getClass().getName());
                }
            }
            // LOG: Stream ended or worker stopping
            System.out.println("Handler (" + threadName + "): Master " + masterAddress + " stream closed or worker stopping.");

        } catch (EOFException e) {
            // LOG: EOF
            System.out.println("Handler (" + threadName + "): Master " + masterAddress + " closed connection (EOF).");
        } catch (SocketException e) {
            // LOG: Socket exception
            // Avoid logging during normal shutdown
            if (isRunning)
                System.out.println("Handler (" + threadName + "): Master connection " + masterAddress + " reset: " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            if (isRunning && !Thread.currentThread().isInterrupted())
                System.err.println("Handler (" + threadName + "): Error communicating with Master " + masterAddress + ": " + e.getMessage());
        } catch (Exception e) {
            if (isRunning && !Thread.currentThread().isInterrupted())
                System.err.println("Handler (" + threadName + "): Unexpected error processing request from Master " + masterAddress + ": " + e.getMessage());
            e.printStackTrace(); // Print stack trace for unexpected errors
        } finally {
            // Socket is closed implicitly by try-with-resources if initialization succeeded
            // If initialization failed, socket might still be open, close it manually.
            if (masterSocket != null && !masterSocket.isClosed()) {
                try {
                    masterSocket.close();
                } catch (IOException e) { /* Ignore */ }
            }
            // LOG: Stopped processing
            System.out.println("Handler (" + threadName + "): Stopped processing for Master " + masterAddress);
        }
    }

    /**
     * Processes different types of BaseRequest objects from Master.
     *
     * @param request          The received BaseRequest object.
     * @param objOut           The ObjectOutputStream to send responses back to the Master.
     * @param outputStreamLock The lock object for synchronizing writes to objOut.
     */
    private void processWorkerRequest(BaseRequest request, ObjectOutputStream objOut, Object outputStreamLock) throws IOException {
        String threadName = Thread.currentThread().getName(); // Get thread name for logging
        // Check specific request types (Master -> Worker DTOs)
        if (request instanceof StoreDataRequest) {
            System.out.println("Handler (" + threadName + "): Processing StoreDataRequest..."); // LOG: Processing StoreData
            storeData(((StoreDataRequest) request).getStore());
            // No response needed?
        } else if (request instanceof UpdateProductRequest) {
            System.out.println("Handler (" + threadName + "): Processing UpdateProductRequest..."); // LOG: Processing UpdateProduct
            updateStoreProduct((UpdateProductRequest) request);
            // No response needed?
        } else if (request instanceof MapFilterTaskRequest) {
            System.out.println("Handler (" + threadName + "): Processing MapFilterTaskRequest..."); // LOG: Processing MapFilterTask
            handleProcessFilterAndSendToReducer((MapFilterTaskRequest) request);
            // Results sent directly to Reducer
        } else if (request instanceof PurchaseRequest) {
            System.out.println("Handler (" + threadName + "): Processing PurchaseRequest..."); // LOG: Processing Purchase
            PurchaseResponse response = processPurchase((PurchaseRequest) request);
            sendResponse(response, objOut, outputStreamLock); // Send response back to Master
        } else {
            System.err.println("Handler (" + threadName + "): Received unhandled BaseRequest type: " + request.getClass().getName());
        }
    }

    /**
     * Handles the MapFilterTaskRequest: filters local stores and sends results to Reducer.
     *
     * @param request The MapFilterTaskRequest object from Master.
     */
    private void handleProcessFilterAndSendToReducer(MapFilterTaskRequest request) {
        String jobId = request.getJobId();
        FilterCriteria criteria = request.getCriteria();
        String threadName = Thread.currentThread().getName();
        // LOG: Start filter processing
        System.out.println("Handler (" + threadName + "): Starting filter logic for Job ID: " + jobId);

        List<String> results = processFilterLogic(criteria); // Perform filtering

        // LOG: Filter logic complete
        System.out.println("Handler (" + threadName + "): Filter logic completed for Job ID: " + jobId + ". Found " + results.size() + " results.");

        // Use try-with-resources for socket and stream to Reducer
        Socket reducerSocket = null; // Declare outside try for logging in catch
        try {
            // LOG: Connecting to Reducer
            System.out.println("Handler (" + threadName + "): Connecting to Reducer at " + this.reducerHost + ":" + this.reducerPort + " for Job ID: " + jobId);
            reducerSocket = new Socket(this.reducerHost, this.reducerPort);
            ObjectOutputStream reducerObjectOut = new ObjectOutputStream(reducerSocket.getOutputStream());

            // Create and send the results message
            WorkerResultsMessage resultsMessage = new WorkerResultsMessage(jobId, results);
            // LOG: Sending results to Reducer
            System.out.println("Handler (" + threadName + ") -> Reducer: Sending WorkerResultsMessage for Job ID: " + jobId + " (" + results.size() + " results)");
            reducerObjectOut.writeObject(resultsMessage);
            reducerObjectOut.flush();
            // LOG: Finished sending results
            System.out.println("Handler (" + threadName + ") -> Reducer: Finished sending results for Job ID: " + jobId);
            // Close the output stream and socket
            reducerObjectOut.close();
            reducerSocket.close();

        } catch (UnknownHostException e) {
            System.err.println("Handler (" + threadName + ") (FILTER Job " + jobId + "): Reducer host not found: " + this.reducerHost);
        } catch (IOException e) {
            System.err.println("Handler (" + threadName + ") (FILTER Job " + jobId + "): Error communicating with Reducer (" + this.reducerHost + ":" + this.reducerPort + "): " + e.getMessage());
            // Ensure socket is closed if partially opened
            if (reducerSocket != null && !reducerSocket.isClosed()) {
                try {
                    reducerSocket.close();
                } catch (IOException ioEx) {
                }
            }
        } catch (Exception e) { // Catch unexpected errors during Reducer communication
            System.err.println("Handler (" + threadName + ") (FILTER Job " + jobId + "): Unexpected error sending results to Reducer: " + e.getMessage());
            e.printStackTrace();
            if (reducerSocket != null && !reducerSocket.isClosed()) {
                try {
                    reducerSocket.close();
                } catch (IOException ioEx) {
                }
            }
        }
    }

    /**
     * Sends a Serializable response object back to the specific Master connection.
     *
     * @param response         The Serializable object to send (e.g., PurchaseResponse).
     * @param objOut           The ObjectOutputStream for the specific Master connection.
     * @param outputStreamLock The lock for the specific Master connection's output stream.
     */
    private void sendResponse(Serializable response, ObjectOutputStream objOut, Object outputStreamLock) {
        String threadName = Thread.currentThread().getName();
        try {
            synchronized (outputStreamLock) {
                // LOG: Sending response to Master
                System.out.println("Handler (" + threadName + ") -> Master: Sending " + response.getClass().getSimpleName() + "...");
                objOut.writeObject(response);
                objOut.flush();
                objOut.reset();
            }
        } catch (IOException e) {
            System.err.println("Handler (" + threadName + "): Error sending response to Master: " + e.getMessage());
            // Let handleMasterConnection handle connection closure on error
        }
    }


    // --- Core Worker Logic Methods (Reduced Printing) ---

    /**
     * Stores or updates data for a given store.
     */
    public boolean storeData(Store store) {
        if (store == null || store.getStoreName() == null || store.getStoreName().trim().isEmpty()) {
            System.err.println("Worker (Port " + port + "): Invalid Store data.");
            return false;
        }
        try {
            store.calculateAndSetPriceCategory();
            synchronized (storesLock) {
                stores.put(store.getStoreName(), store);
            }
            System.out.println("Worker (Port " + port + "): Stored/Updated store: " + store.getStoreName()); // LOG: Store update
            return true;
        } catch (Exception e) {
            System.err.println("Worker (Port " + port + "): Error processing STORE_DATA for " + store.getStoreName() + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Updates a product within a store (add, remove, update stock).
     */
    public boolean updateStoreProduct(UpdateProductRequest request) {
        if (request == null || request.getStoreName() == null) {
            System.err.println("Worker (Port " + port + "): Invalid UpdateProductRequest.");
            return false;
        }
        String storeName = request.getStoreName();
        Store store;
        synchronized (storesLock) {
            store = stores.get(storeName);
        }
        if (store == null) {
            System.err.println("Worker (Port " + port + "): Store not found for update: " + storeName);
            return false;
        }
        String productName = request.getProductName();
        boolean success = false;
        try {
            switch (request.getAction()) {
                case ADD:
                    if (productName == null || request.getProductType() == null) return false;
                    Product np = new Product(productName, request.getProductType(), request.getAmount(), request.getPrice());
                    store.addProduct(np);
                    success = true;
                    break;
                case REMOVE:
                    if (productName == null) return false;
                    Product rem = store.removeProduct(productName);
                    success = (rem != null);
                    if (!success)
                        System.err.println("Worker (Port " + port + "): Product '" + productName + "' not found in '" + storeName + "' for removal.");
                    break;
                case UPDATE_STOCK:
                    if (productName == null) return false;
                    Product pu = store.getProduct(productName);
                    if (pu != null) {
                        pu.setStock(request.getAmount());
                        success = true;
                    } else {
                        System.err.println("Worker (Port " + port + "): Product '" + productName + "' not found in '" + storeName + "' for stock update.");
                    }
                    break;
                default:
                    System.err.println("Worker (Port " + port + "): Unknown action: " + request.getAction());
                    return false;
            }
            if (success)
                System.out.println("Worker (Port " + port + "): Action " + request.getAction() + " successful for product '" + productName + "' in store '" + storeName + "'."); // LOG: Product update
            return success;
        } catch (Exception e) {
            System.err.println("Worker (Port " + port + "): Error during updateStoreProduct for " + storeName + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Filters stores based on criteria.
     */
    public List<String> processFilterLogic(FilterCriteria criteria) {
        List<String> results = new ArrayList<>();
        if (criteria == null) {
            System.err.println("Worker (Port " + port + "): Null FilterCriteria.");
            return results;
        }
        List<Store> storesToCheck;
        synchronized (storesLock) {
            storesToCheck = new ArrayList<>(stores.values());
        }
        try {
            for (Store store : storesToCheck) {
                double distance = calculateDistance(criteria.getClientLatitude(), criteria.getClientLongitude(), store.getLatitude(), store.getLongitude());
                if (distance > criteria.getMaxDistance()) continue;
                List<String> targetFoodCategories = criteria.getFoodCategories();
                if (targetFoodCategories != null && !targetFoodCategories.isEmpty() && !targetFoodCategories.contains(store.getFoodCategory()))
                    continue;
                if (store.getStars() < criteria.getMinStars()) continue;
                List<String> targetPriceCategories = criteria.getPriceCategories();
                if (targetPriceCategories != null && !targetPriceCategories.isEmpty() && !targetPriceCategories.contains(store.getPriceCategory()))
                    continue;
                results.add(store.toString()); // Assuming Store.toString() is suitable
            }
        } catch (Exception e) {
            System.err.println("Worker (Port " + port + "): Error during filtering: " + e.getMessage());
            return new ArrayList<>();
        }
        return results;
    }

    /**
     * Processes a purchase request.
     */
    public PurchaseResponse processPurchase(PurchaseRequest request) {
        if (request == null || request.getStoreName() == null || request.getProductName() == null) {
            return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Invalid purchase request.");
        }
        String storeName = request.getStoreName();
        String productName = request.getProductName();
        int quantity = request.getQuantity();
        if (quantity <= 0) {
            return new PurchaseResponse(PurchaseResponse.Status.FAIL_INVALID_QTY);
        }
        Store store;
        synchronized (storesLock) {
            store = stores.get(storeName);
        }
        if (store == null) {
            return new PurchaseResponse(PurchaseResponse.Status.FAIL_STORE_NOT_FOUND);
        }
        Product product = store.getProduct(productName);
        if (product == null) {
            return new PurchaseResponse(PurchaseResponse.Status.FAIL_PRODUCT_NOT_FOUND);
        }
        boolean success = product.decreaseStock(quantity); // Must be synchronized
        // LOG: Purchase attempt result
        System.out.println("Worker (Port " + port + "): Purchase attempt for " + quantity + "x '" + productName + "' from '" + storeName + "'. Success: " + success);
        return success ? new PurchaseResponse(PurchaseResponse.Status.OK) : new PurchaseResponse(PurchaseResponse.Status.FAIL_STOCK);
    }

    /**
     * Calculates distance between two lat/lon points.
     */
    private static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371;
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    // Main method remains the same
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java gr.aueb.service.Worker <port> <reducerHost> <reducerPort>");
            System.exit(1);
        }
        int port;
        String reducerHost;
        int reducerPort;
        try {
            port = Integer.parseInt(args[0]);
            reducerHost = args[1];
            reducerPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid Port format.");
            System.exit(1);
            return;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Missing arguments.");
            System.exit(1);
            return;
        }
        Worker worker = new Worker(port, reducerHost, reducerPort);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Worker shutdown hook for port " + port);
            worker.stop();
        }));
        worker.run();
    }
}
