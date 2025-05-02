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
 * Reduced console output.
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
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Worker (Port " + port + "): Listening for Master connections...");

            while (isRunning && !Thread.currentThread().isInterrupted()) {
                Socket masterSocket = null;
                try {
                    masterSocket = serverSocket.accept();
                    if (!isRunning) {
                        if (masterSocket != null) masterSocket.close();
                        break;
                    }

                    Socket finalMasterSocket = masterSocket;
                    Thread handlerThread = new Thread(() -> {
                        handleMasterConnection(finalMasterSocket);
                        connectionThreads.remove(Thread.currentThread());
                    }, "WorkerHandler-" + masterSocket.getRemoteSocketAddress());
                    connectionThreads.add(handlerThread);
                    handlerThread.start();

                } catch (SocketException e) {
                    if (!isRunning) System.out.println("Worker (Port " + port + "): Server socket closed, stopping.");
                    else System.err.println("Worker (Port " + port + "): SocketException accepting: " + e.getMessage());
                } catch (IOException e) {
                    if (isRunning) System.err.println("Worker (Port " + port + "): Error accepting connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Worker (Port " + port + "): Could not start server: " + e.getMessage());
        } finally {
            System.out.println("Worker (Port " + port + "): Shutting down listener loop.");
            if (serverSocket != null && !serverSocket.isClosed()) {
                try { serverSocket.close(); } catch (IOException e) { /* ignore */ }
            }
            synchronized(connectionThreads) {
                if (!connectionThreads.isEmpty()) {
                    try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
            }
            System.out.println("Worker (Port " + port + "): Finished run method.");
        }
    }

    public void stop() {
        System.out.println("Worker (Port " + port + "): Received stop signal.");
        isRunning = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try { serverSocket.close(); } catch (IOException e) { /* ignore */ }
        }
        synchronized (connectionThreads) {
            List<Thread> threadsToInterrupt = new ArrayList<>(connectionThreads);
            for (Thread t : threadsToInterrupt) {
                if (t.isAlive()) t.interrupt();
            }
            connectionThreads.clear();
        }
    }


    /** Handles communication with a single Master connection. */
    private void handleMasterConnection(Socket masterSocket) {
        String masterAddress = masterSocket.getInetAddress().getHostAddress();
        try (
                ObjectOutputStream objOut = new ObjectOutputStream(masterSocket.getOutputStream());
                ObjectInputStream objIn = new ObjectInputStream(masterSocket.getInputStream())
        ) {
            final Object outputStreamLock = new Object();
            Object requestObject;
            while (isRunning && !Thread.currentThread().isInterrupted() && (requestObject = objIn.readObject()) != null) {
                if (requestObject instanceof BaseRequest) {
                    processWorkerRequest((BaseRequest) requestObject, objOut, outputStreamLock);
                } else {
                    System.err.println("Handler (" + Thread.currentThread().getName() + "): Unknown object type from Master: " + requestObject.getClass().getName());
                }
            }
        } catch (EOFException e) {
            // Master closed connection normally
        } catch (SocketException e) {
            if (isRunning) System.out.println("Handler (" + Thread.currentThread().getName() + "): Master connection reset: " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            if (isRunning && !Thread.currentThread().isInterrupted())
                System.err.println("Handler (" + Thread.currentThread().getName() + "): Error communicating with Master " + masterAddress + ": " + e.getMessage());
        } catch (Exception e) {
            if (isRunning && !Thread.currentThread().isInterrupted())
                System.err.println("Handler (" + Thread.currentThread().getName() + "): Unexpected error processing request from Master " + masterAddress + ": " + e.getMessage());
        } finally {
            if (masterSocket != null && !masterSocket.isClosed()) {
                try { masterSocket.close(); } catch (IOException e) { /* Ignore */ }
            }
        }
    }

    /** Processes different BaseRequest types from Master. */
    private void processWorkerRequest(BaseRequest request, ObjectOutputStream objOut, Object outputStreamLock) throws IOException {
        if (request instanceof StoreDataRequest) {
            storeData(((StoreDataRequest) request).getStore());
        } else if (request instanceof UpdateProductRequest) {
            updateStoreProduct((UpdateProductRequest) request);
        } else if (request instanceof MapFilterTaskRequest) { // *** CHANGED TYPE CHECK ***
            // Cast to the correct type and call handler
            handleProcessFilterAndSendToReducer((MapFilterTaskRequest) request); // *** CAST TO CORRECT TYPE ***
        } else if (request instanceof PurchaseRequest) {
            PurchaseResponse response = processPurchase((PurchaseRequest) request);
            sendResponse(response, objOut, outputStreamLock);
        } else {
            System.err.println("Handler: Received unhandled BaseRequest type: " + request.getClass().getName());
        }
    }

    /**
     * Handles the MapFilterTaskRequest: filters local stores and sends results to Reducer.
     * @param request The MapFilterTaskRequest object from Master. // *** CHANGED PARAMETER TYPE ***
     */
    private void handleProcessFilterAndSendToReducer(MapFilterTaskRequest request) { // *** CHANGED PARAMETER TYPE ***
        String jobId = request.getJobId();
        FilterCriteria criteria = request.getCriteria();
        // Reduced printing

        List<String> results = processFilterLogic(criteria); // Perform filtering

        // Reduced printing

        try (Socket reducerSocket = new Socket(this.reducerHost, this.reducerPort); // Use fields from constructor
             ObjectOutputStream reducerObjectOut = new ObjectOutputStream(reducerSocket.getOutputStream()))
        {
            WorkerResultsMessage resultsMessage = new WorkerResultsMessage(jobId, results);
            reducerObjectOut.writeObject(resultsMessage);
            reducerObjectOut.flush();
            // No WorkerDoneMessage
        } catch (UnknownHostException e) {
            System.err.println("Handler (" + Thread.currentThread().getName() + "): Reducer host " + this.reducerHost + " not found (Job " + jobId + ")");
        } catch (IOException e) {
            System.err.println("Handler (" + Thread.currentThread().getName() + "): Error sending results to Reducer " + this.reducerHost + ":" + this.reducerPort + " (Job " + jobId + "): " + e.getMessage());
        }
    }

    /** Sends a response object back to the Master. */
    private void sendResponse(Serializable response, ObjectOutputStream objOut, Object outputStreamLock) {
        try {
            synchronized (outputStreamLock) {
                objOut.writeObject(response);
                objOut.flush();
                objOut.reset();
            }
        } catch (IOException e) {
            System.err.println("Handler (" + Thread.currentThread().getName() + "): Error sending response to Master: " + e.getMessage());
        }
    }


    // --- Core Worker Logic Methods (No change needed below this line for this refactor) ---

    public boolean storeData(Store store) {
        if (store == null || store.getStoreName() == null || store.getStoreName().trim().isEmpty()) {
            System.err.println("Worker (Port " + port + "): Invalid Store data."); return false;
        }
        try {
            store.calculateAndSetPriceCategory();
            synchronized (storesLock) { stores.put(store.getStoreName(), store); }
            return true;
        } catch (Exception e) {
            System.err.println("Worker (Port " + port + "): Error processing STORE_DATA for " + store.getStoreName() + ": " + e.getMessage()); return false;
        }
    }

    public boolean updateStoreProduct(UpdateProductRequest request) {
        if (request == null || request.getStoreName() == null) {
            System.err.println("Worker (Port " + port + "): Invalid UpdateProductRequest."); return false;
        }
        String storeName = request.getStoreName();
        Store store;
        synchronized (storesLock) { store = stores.get(storeName); }
        if (store == null) {
            System.err.println("Worker (Port " + port + "): Store not found for update: " + storeName); return false;
        }
        String productName = request.getProductName();
        try {
            switch (request.getAction()) {
                case ADD:
                    if (productName == null || request.getProductType() == null) return false;
                    Product np = new Product(productName, request.getProductType(), request.getAmount(), request.getPrice());
                    store.addProduct(np); return true;
                case REMOVE:
                    if (productName == null) return false;
                    Product rem = store.removeProduct(productName);
                    if (rem == null) System.err.println("Worker (Port " + port + "): Product '" + productName + "' not found in '" + storeName + "' for removal.");
                    return rem != null;
                case UPDATE_STOCK:
                    if (productName == null) return false;
                    Product pu = store.getProduct(productName);
                    if (pu != null) { pu.setStock(request.getAmount()); return true; }
                    else { System.err.println("Worker (Port " + port + "): Product '" + productName + "' not found in '" + storeName + "' for stock update."); return false; }
                default: System.err.println("Worker (Port " + port + "): Unknown action: " + request.getAction()); return false;
            }
        } catch (Exception e) { System.err.println("Worker (Port " + port + "): Error during updateStoreProduct for " + storeName + ": " + e.getMessage()); return false; }
    }

    public List<String> processFilterLogic(FilterCriteria criteria) {
        List<String> results = new ArrayList<>();
        if (criteria == null) { System.err.println("Worker (Port " + port + "): Null FilterCriteria."); return results; }
        List<Store> storesToCheck;
        synchronized (storesLock) { storesToCheck = new ArrayList<>(stores.values()); }
        try {
            for (Store store : storesToCheck) {
                double distance = calculateDistance(criteria.getClientLatitude(), criteria.getClientLongitude(), store.getLatitude(), store.getLongitude());
                if (distance > criteria.getMaxDistance()) continue;
                List<String> targetFoodCategories = criteria.getFoodCategories();
                if (targetFoodCategories != null && !targetFoodCategories.isEmpty() && !targetFoodCategories.contains(store.getFoodCategory())) continue;
                if (store.getStars() < criteria.getMinStars()) continue;
                List<String> targetPriceCategories = criteria.getPriceCategories();
                if (targetPriceCategories != null && !targetPriceCategories.isEmpty() && !targetPriceCategories.contains(store.getPriceCategory())) continue;
                results.add(store.toString());
            }
        } catch (Exception e) { System.err.println("Worker (Port " + port + "): Error during filtering: " + e.getMessage()); return new ArrayList<>(); }
        return results;
    }

    public PurchaseResponse processPurchase(PurchaseRequest request) {
        if (request == null || request.getStoreName() == null || request.getProductName() == null) {
            return new PurchaseResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR, "Invalid purchase request."); }
        String storeName = request.getStoreName(); String productName = request.getProductName(); int quantity = request.getQuantity();
        if (quantity <= 0) { return new PurchaseResponse(PurchaseResponse.Status.FAIL_INVALID_QTY); }
        Store store; synchronized (storesLock) { store = stores.get(storeName); }
        if (store == null) { return new PurchaseResponse(PurchaseResponse.Status.FAIL_STORE_NOT_FOUND); }
        Product product = store.getProduct(productName);
        if (product == null) { return new PurchaseResponse(PurchaseResponse.Status.FAIL_PRODUCT_NOT_FOUND); }
        boolean success = product.decreaseStock(quantity); // Must be synchronized
        return success ? new PurchaseResponse(PurchaseResponse.Status.OK) : new PurchaseResponse(PurchaseResponse.Status.FAIL_STOCK);
    }

    private static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; double latDistance = Math.toRadians(lat2 - lat1); double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)); return R * c;
    }

    public static void main(String[] args) {
        if (args.length < 3) { System.err.println("Usage: java gr.aueb.service.Worker <port> <reducerHost> <reducerPort>"); System.exit(1); }
        int port; String reducerHost; int reducerPort;
        try { port = Integer.parseInt(args[0]); reducerHost = args[1]; reducerPort = Integer.parseInt(args[2]); }
        catch (NumberFormatException e) { System.err.println("Invalid Port format."); System.exit(1); return; }
        catch (ArrayIndexOutOfBoundsException e) { System.err.println("Missing arguments."); System.exit(1); return; }
        Worker worker = new Worker(port, reducerHost, reducerPort);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { System.out.println("Worker shutdown hook for port " + port); worker.stop(); }));
        worker.run();
    }
}