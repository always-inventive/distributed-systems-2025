package gr.aueb.manager;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import com.google.gson.Gson; // Using Gson for parsing Store JSON from AddStoreManagerRequest
import com.google.gson.JsonSyntaxException;

import gr.aueb.dtos.BaseRequest;
import gr.aueb.manager.dtos.*;
import gr.aueb.model.Store;
import gr.aueb.service.Master;

/**
 * Handles communication with a single connected Manager client
 * using Object Streams. Interacts with the Master instance.
 * Receives the first request object from the Master during construction.
 */
public class ManagerRequestHandler implements Runnable, Closeable {
    private final Socket clientSocket;
    private final Master master; // Reference to the Master instance
    private final ObjectOutputStream objectOut; // To Manager Client
    private final ObjectInputStream objectIn;   // From Manager Client
    private final BaseManagerRequest firstRequest; // Store the first request received
    private final Object outputStreamLock = new Object(); // Lock for sending responses to this manager
    private final Gson gson = new Gson(); // For parsing Store JSON in AddStoreManagerRequest

    /**
     * Constructor for the ManagerRequestHandler.
     * Receives the already established streams and the first request object read by the Master.
     *
     * @param socket       The client socket connection.
     * @param master       A reference to the Master instance for coordination.
     * @param objectIn     The already initialized ObjectInputStream from the manager client.
     * @param objectOut    The already initialized ObjectOutputStream to the manager client.
     * @param firstRequest The first BaseManagerRequest object read by the Master.
     */
    public ManagerRequestHandler(Socket socket, Master master, ObjectInputStream objectIn, ObjectOutputStream objectOut, BaseManagerRequest firstRequest) {
        this.clientSocket = socket;
        this.master = master;
        this.objectIn = objectIn;
        this.objectOut = objectOut;
        this.firstRequest = firstRequest; // Store the first request
    }

    @Override
    public void run() {
        String clientAddress = clientSocket.getInetAddress().getHostAddress();
        System.out.println("ManagerRequestHandler: Starting request processing for MANAGER " + clientAddress);
        try {
            // --- Process the first request received from Master ---
            if (this.firstRequest != null) {
                System.out.println("ManagerRequestHandler: Processing first request of type: " + this.firstRequest.getClass().getSimpleName());
                processRequest(this.firstRequest);
            } else {
                System.err.println("ManagerRequestHandler: Started without a valid first request for " + clientAddress);
                // Decide how to handle this: maybe close? For now, proceed to loop.
            }

            // --- Loop to process subsequent requests ---
            Object requestObject;
            // Read subsequent request objects from the Manager Client
            while ((requestObject = objectIn.readObject()) != null) {
                // Check if the received object is a known Manager request type
                if (requestObject instanceof BaseManagerRequest) {
                    System.out.println("ManagerRequestHandler: Received subsequent request: " + requestObject.getClass().getSimpleName());
                    processRequest((BaseManagerRequest) requestObject);
                } else {
                    // Log if an unexpected object type is received later
                    System.err.println("ManagerRequestHandler: Received unknown object type from manager " + clientAddress + ": " + requestObject.getClass().getName());
                    sendResponse(new ManagerActionResponse(false, "Invalid object type received."));
                    // break; // Example: Stop processing for this manager
                }
            }
            // readObject() returned null, indicating end of stream
            System.out.println("ManagerRequestHandler: Manager client " + clientAddress + " stream closed (read null).");

        } catch (EOFException e) {
            System.out.println("ManagerRequestHandler: Manager client " + clientAddress + " closed the connection (EOF).");
        } catch (SocketException e) {
            System.out.println("ManagerRequestHandler: Manager connection " + clientAddress + " closed or reset: " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("ManagerRequestHandler: Error communicating with manager " + clientAddress + ": " + e.getMessage());
            // e.printStackTrace(); // Uncomment for detailed debugging
        } catch (Exception e) { // Catch any other unexpected errors
            System.err.println("ManagerRequestHandler: Unexpected error processing request from manager " + clientAddress + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            close(); // Ensure resources are closed when the handler finishes
        }
        System.out.println("ManagerRequestHandler: Stopped processing for manager " + clientAddress);
    }

    /**
     * Processes a received BaseManagerRequest object by determining its specific type
     * and calling the appropriate handling method.
     *
     * @param request The BaseManagerRequest object received from the manager client.
     */
    private void processRequest(BaseManagerRequest request) {
        try {
            // Determine the specific type of manager request and call the handler
            if (request instanceof AddStoreManagerRequest) {
                handleAddStore((AddStoreManagerRequest) request);
            } else if (request instanceof AddProductManagerRequest) {
                handleAddProduct((AddProductManagerRequest) request);
            } else if (request instanceof RemoveProductManagerRequest) {
                handleRemoveProduct((RemoveProductManagerRequest) request);
            } else if (request instanceof UpdateStockManagerRequest) {
                handleUpdateStock((UpdateStockManagerRequest) request);
            }
            // Add else-if blocks here for other manager request types
            // else if (request instanceof GetSalesReportManagerRequest) { ... }
            else {
                System.err.println("ManagerRequestHandler: Received valid but unhandled BaseManagerRequest type: " + request.getClass().getName());
                sendResponse(new ManagerActionResponse(false, "Unsupported request type."));
            }
        } catch (Exception e) {
            // Catch errors specific to request processing logic
            System.err.println("ManagerRequestHandler: Error processing request (" + request.getClass().getSimpleName() + "): " + e.getMessage());
            e.printStackTrace();
            // Send a generic error response back to the manager
            sendResponse(new ManagerActionResponse(false, "Internal server error processing request."));
        }
    }


    // --- Request Handling Methods ---

    /**
     * Handles AddStoreManagerRequest. Parses the JSON, determines the target worker,
     * creates a StoreDataRequest (Master->Worker), and forwards it via the Master.
     * Sends a ManagerActionResponse back to the manager client.
     * @param request The AddStoreManagerRequest received.
     */
    private void handleAddStore(AddStoreManagerRequest request) {
        String jsonContent = request.getStoreJson();
        System.out.println("ManagerRequestHandler: Processing ADD_STORE request.");
        boolean overallSuccess = false;
        String message = "Failed to add store."; // Default failure message

        if (jsonContent == null || jsonContent.trim().isEmpty()) {
            message = "Error: Received empty store data.";
            System.err.println("ManagerRequestHandler (ADD_STORE): " + message);
        } else {
            try {
                // 1. Parse JSON to Store object using Gson
                Store store = gson.fromJson(jsonContent, Store.class);
                if (store == null || store.getStoreName() == null || store.getStoreName().trim().isEmpty()) {
                    message = "Error: Could not parse valid Store object from JSON (missing StoreName?).";
                    System.err.println("ManagerRequestHandler (ADD_STORE): " + message);
                } else {
                    // 2. Get target worker ID using Master's method
                    int workerId = master.getWorkerIdForStore(store.getStoreName());

                    // 3. Create StoreDataRequest DTO (Master -> Worker DTO)
                    // This DTO contains the fully parsed Store object
                    StoreDataRequest workerRequest = new StoreDataRequest(store);

                    // 4. Forward object request to worker via Master's method
                    overallSuccess = master.forwardRequestObjectToWorker(workerId, workerRequest);
                    message = overallSuccess ? "Store add/update request forwarded successfully to worker " + workerId + "."
                            : "Failed to forward store add/update request to worker " + workerId + ".";
                    System.out.println("ManagerRequestHandler (ADD_STORE): Forwarded StoreDataRequest to Worker " + workerId + " for store '" + store.getStoreName() + "' - Success: " + overallSuccess);
                }
            } catch (JsonSyntaxException e) {
                message = "Error: Invalid JSON format received. Details: " + e.getMessage();
                System.err.println("ManagerRequestHandler (ADD_STORE): " + message);
            } catch (IllegalStateException e) { // Thrown by getWorkerIdForStore if no workers
                message = "Error: No workers available to handle the request.";
                System.err.println("ManagerRequestHandler (ADD_STORE): " + message + " " + e.getMessage());
            } catch (Exception e) { // Catch other potential errors during processing
                message = "Error processing add store request: " + e.getMessage();
                System.err.println("ManagerRequestHandler (ADD_STORE): Unexpected error: " + message);
                e.printStackTrace();
            }
        }
        // Send response back to manager client
        sendResponse(new ManagerActionResponse(overallSuccess, message));
    }

    /**
     * Handles AddProductManagerRequest. Determines the target worker,
     * creates an UpdateProductRequest (Master->Worker) with ADD action, and forwards it.
     * Sends a ManagerActionResponse back to the manager client.
     * @param request The AddProductManagerRequest received.
     */
    private void handleAddProduct(AddProductManagerRequest request) {
        System.out.println("ManagerRequestHandler: Processing ADD_PRODUCT request for store '" + request.getStoreName() + "', product '" + request.getProductName() + "'.");
        boolean overallSuccess = false;
        String message = "Failed to forward add product request.";
        try {
            // Basic validation
            if (request.getStoreName() == null || request.getStoreName().trim().isEmpty() ||
                    request.getProductName() == null || request.getProductName().trim().isEmpty() ||
                    request.getProductType() == null || request.getProductType().trim().isEmpty() ||
                    request.getAmount() < 0 || request.getPrice() < 0) {
                message = "Error: Invalid data in AddProductManagerRequest.";
                System.err.println("ManagerRequestHandler (ADD_PRODUCT): " + message);
            } else {
                int workerId = master.getWorkerIdForStore(request.getStoreName());
                // Create UpdateProductRequest DTO for ADD action (Master -> Worker DTO)
                // Note: Using gr.aueb.manager.dtos.UpdateProductRequest here, assuming it's the correct Master->Worker DTO
                gr.aueb.manager.dtos.UpdateProductRequest workerRequest = new gr.aueb.manager.dtos.UpdateProductRequest(
                        gr.aueb.manager.dtos.UpdateProductRequest.Action.ADD,
                        request.getStoreName(),
                        request.getProductName(),
                        request.getProductType(),
                        request.getAmount(),
                        request.getPrice()
                );
                // Forward object request to worker via Master
                overallSuccess = master.forwardRequestObjectToWorker(workerId, workerRequest);
                message = overallSuccess ? "Add product request forwarded successfully to worker " + workerId + "."
                        : "Failed to forward add product request to worker " + workerId + ".";
                System.out.println("ManagerRequestHandler (ADD_PRODUCT): Forwarded UpdateProductRequest(ADD) to Worker " + workerId + " - Success: " + overallSuccess);
            }
        } catch (IllegalStateException e) {
            message = "Error: No workers available.";
            System.err.println("ManagerRequestHandler (ADD_PRODUCT): " + message + " " + e.getMessage());
        } catch (Exception e) {
            message = "Error processing add product request: " + e.getMessage();
            System.err.println("ManagerRequestHandler (ADD_PRODUCT): Unexpected error: " + message);
            e.printStackTrace();
        }
        // Send response back to manager client
        sendResponse(new ManagerActionResponse(overallSuccess, message));
    }

    /**
     * Handles RemoveProductManagerRequest. Determines the target worker,
     * creates an UpdateProductRequest (Master->Worker) with REMOVE action, and forwards it.
     * Sends a ManagerActionResponse back to the manager client.
     * @param request The RemoveProductManagerRequest received.
     */
    private void handleRemoveProduct(RemoveProductManagerRequest request) {
        System.out.println("ManagerRequestHandler: Processing REMOVE_PRODUCT request for store '" + request.getStoreName() + "', product '" + request.getProductName() + "'.");
        boolean overallSuccess = false;
        String message = "Failed to forward remove product request.";
        try {
            // Basic validation
            if (request.getStoreName() == null || request.getStoreName().trim().isEmpty() ||
                    request.getProductName() == null || request.getProductName().trim().isEmpty()) {
                message = "Error: Invalid data in RemoveProductManagerRequest.";
                System.err.println("ManagerRequestHandler (REMOVE_PRODUCT): " + message);
            } else {
                int workerId = master.getWorkerIdForStore(request.getStoreName());
                // Create UpdateProductRequest DTO for REMOVE action (Master -> Worker DTO)
                gr.aueb.manager.dtos.UpdateProductRequest workerRequest = new gr.aueb.manager.dtos.UpdateProductRequest(
                        gr.aueb.manager.dtos.UpdateProductRequest.Action.REMOVE,
                        request.getStoreName(),
                        request.getProductName()
                );
                // Forward object request to worker via Master
                overallSuccess = master.forwardRequestObjectToWorker(workerId, workerRequest);
                message = overallSuccess ? "Remove product request forwarded successfully to worker " + workerId + "."
                        : "Failed to forward remove product request to worker " + workerId + ".";
                System.out.println("ManagerRequestHandler (REMOVE_PRODUCT): Forwarded UpdateProductRequest(REMOVE) to Worker " + workerId + " - Success: " + overallSuccess);
            }
        } catch (IllegalStateException e) {
            message = "Error: No workers available.";
            System.err.println("ManagerRequestHandler (REMOVE_PRODUCT): " + message + " " + e.getMessage());
        } catch (Exception e) {
            message = "Error processing remove product request: " + e.getMessage();
            System.err.println("ManagerRequestHandler (REMOVE_PRODUCT): Unexpected error: " + message);
            e.printStackTrace();
        }
        // Send response back to manager client
        sendResponse(new ManagerActionResponse(overallSuccess, message));
    }

    /**
     * Handles UpdateStockManagerRequest. Determines the target worker,
     * creates an UpdateProductRequest (Master->Worker) with UPDATE_STOCK action, and forwards it.
     * Sends a ManagerActionResponse back to the manager client.
     * @param request The UpdateStockManagerRequest received.
     */
    private void handleUpdateStock(UpdateStockManagerRequest request) {
        System.out.println("ManagerRequestHandler: Processing UPDATE_STOCK request for store '" + request.getStoreName() + "', product '" + request.getProductName() + "'.");
        boolean overallSuccess = false;
        String message = "Failed to forward update stock request.";
        try {
            // Basic validation
            if (request.getStoreName() == null || request.getStoreName().trim().isEmpty() ||
                    request.getProductName() == null || request.getProductName().trim().isEmpty() ||
                    request.getNewAmount() < 0) { // Allow 0 amount
                message = "Error: Invalid data in UpdateStockManagerRequest.";
                System.err.println("ManagerRequestHandler (UPDATE_STOCK): " + message);
            } else {
                int workerId = master.getWorkerIdForStore(request.getStoreName());
                // Create UpdateProductRequest DTO for UPDATE_STOCK action (Master -> Worker DTO)
                gr.aueb.manager.dtos.UpdateProductRequest workerRequest = new gr.aueb.manager.dtos.UpdateProductRequest(
                        gr.aueb.manager.dtos.UpdateProductRequest.Action.UPDATE_STOCK,
                        request.getStoreName(),
                        request.getProductName(),
                        request.getNewAmount()
                );
                // Forward object request to worker via Master
                overallSuccess = master.forwardRequestObjectToWorker(workerId, workerRequest);
                message = overallSuccess ? "Update stock request forwarded successfully to worker " + workerId + "."
                        : "Failed to forward update stock request to worker " + workerId + ".";
                System.out.println("ManagerRequestHandler (UPDATE_STOCK): Forwarded UpdateProductRequest(UPDATE_STOCK) to Worker " + workerId + " - Success: " + overallSuccess);
            }
        } catch (IllegalStateException e) {
            message = "Error: No workers available.";
            System.err.println("ManagerRequestHandler (UPDATE_STOCK): " + message + " " + e.getMessage());
        } catch (Exception e) {
            message = "Error processing update stock request: " + e.getMessage();
            System.err.println("ManagerRequestHandler (UPDATE_STOCK): Unexpected error: " + message);
            e.printStackTrace();
        }
        // Send response back to manager client
        sendResponse(new ManagerActionResponse(overallSuccess, message));
    }

    /**
     * Sends a Serializable response object back to the Manager Client.
     * Synchronized on the outputStreamLock for thread safety.
     * @param response The Serializable object to send (should be ManagerActionResponse).
     */
    private void sendResponse(Serializable response) {
        if (response == null) {
            System.err.println("ManagerRequestHandler: Attempted to send a null response to " + clientSocket.getInetAddress().getHostAddress());
            return;
        }
        try {
            // Synchronize writes for this specific manager client
            synchronized (outputStreamLock) {
                System.out.println("ManagerRequestHandler -> " + clientSocket.getInetAddress().getHostAddress() + ": Sending " + response.getClass().getSimpleName() + "...");
                objectOut.writeObject(response);
                objectOut.flush(); // Ensure data is sent
                objectOut.reset(); // Reset object stream state
            }
        } catch (IOException e) {
            System.err.println("ManagerRequestHandler: Error sending response ("+ response.getClass().getSimpleName() +") to manager " + clientSocket.getInetAddress().getHostAddress() + ": " + e.getMessage());
            // Close the connection if sending fails
            close();
        }
    }

    /**
     * Closes the manager client connection resources (streams and socket).
     * Called when the handler finishes or an unrecoverable error occurs.
     */
    @Override
    public void close() {
        // Avoid closing multiple times
        if (clientSocket == null || clientSocket.isClosed()) {
            return;
        }
        String clientAddress = clientSocket.getInetAddress().getHostAddress();
        System.out.println("ManagerRequestHandler: Closing connection resources for manager " + clientAddress);
        // Close streams first
        try { if (objectOut != null) objectOut.close(); } catch (IOException e) {/* ignore */}
        try { if (objectIn != null) objectIn.close(); } catch (IOException e) {/* ignore */}
        // Then close socket
        try { clientSocket.close(); }
        catch (IOException e) { System.err.println("ManagerRequestHandler: Error closing manager socket (" + clientAddress + "): " + e.getMessage()); }
    }
}
