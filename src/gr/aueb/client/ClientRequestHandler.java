package gr.aueb.client;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

import gr.aueb.client.dtos.*;
import gr.aueb.client.dtos.PurchaseResponse;
import gr.aueb.dtos.BaseRequest;
import gr.aueb.model.FilterCriteria;
import gr.aueb.service.Master;

/**
 * Handles communication with a single connected regular client (e.g., Android App, Console App)
 * using Object Streams. Interacts with the Master instance.
 * Receives the first request object from the Master during construction.
 */
public class ClientRequestHandler implements Runnable, Closeable {
    private final Socket clientSocket;
    private final Master master; // Reference to the Master instance
    private final ObjectOutputStream objectOut; // To Client
    private final ObjectInputStream objectIn;   // From Client
    private final BaseClientRequest firstRequest; // Store the first request received
    private final Object outputStreamLock = new Object(); // Lock for sending responses to this client

    /**
     * Constructor for the ClientRequestHandler.
     * Receives the already established streams and the first request object read by the Master.
     *
     * @param socket       The client socket connection.
     * @param master       A reference to the Master instance for coordination.
     * @param objectIn     The already initialized ObjectInputStream from the client.
     * @param objectOut    The already initialized ObjectOutputStream to the client.
     * @param firstRequest The first BaseClientRequest object read by the Master.
     */
    public ClientRequestHandler(Socket socket, Master master, ObjectInputStream objectIn, ObjectOutputStream objectOut, BaseClientRequest firstRequest) {
        this.clientSocket = socket;
        this.master = master;
        this.objectIn = objectIn;
        this.objectOut = objectOut;
        this.firstRequest = firstRequest; // Store the first request
    }

    @Override
    public void run() {
        String clientAddress = clientSocket.getInetAddress().getHostAddress();
        System.out.println("ClientRequestHandler: Starting request processing for CLIENT " + clientAddress);
        try {
            // --- Process the first request received from Master ---
            if (this.firstRequest != null) {
                System.out.println("ClientRequestHandler: Processing first request of type: " + this.firstRequest.getClass().getSimpleName());
                processRequest(this.firstRequest);
            } else {
                System.err.println("ClientRequestHandler: Started without a valid first request for " + clientAddress);
                // Decide how to handle this: maybe close? For now, proceed to loop.
            }

            // --- Loop to process subsequent requests ---
            Object requestObject;
            // Read subsequent request objects from the Client
            while ((requestObject = objectIn.readObject()) != null) {
                // Check if the received object is a known Client request type
                if (requestObject instanceof BaseClientRequest) {
                    System.out.println("ClientRequestHandler: Received subsequent request: " + requestObject.getClass().getSimpleName());
                    processRequest((BaseClientRequest) requestObject);
                } else {
                    // Log if an unexpected object type is received later
                    System.err.println("ClientRequestHandler: Received unknown object type from client " + clientAddress + ": " + requestObject.getClass().getName());
                    // Optionally send an error response or close connection
                    // sendResponse(new ErrorResponse("Invalid object type received."));
                    // break; // Example: Stop processing for this client
                }
            }
            // readObject() returned null, indicating end of stream
            System.out.println("ClientRequestHandler: Client " + clientAddress + " stream closed (read null).");

        } catch (EOFException e) {
            System.out.println("ClientRequestHandler: Client " + clientAddress + " closed the connection (EOF).");
        } catch (SocketException e) {
            // Handle cases where the socket is closed unexpectedly
            System.out.println("ClientRequestHandler: Client connection " + clientAddress + " closed or reset: " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            // Handle errors during communication or deserialization
            System.err.println("ClientRequestHandler: Error communicating with client " + clientAddress + ": " + e.getMessage());
            // e.printStackTrace(); // Uncomment for detailed debugging
        } catch (Exception e) { // Catch any other unexpected errors
            System.err.println("ClientRequestHandler: Unexpected error processing request from client " + clientAddress + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            close(); // Ensure resources are closed when the handler finishes
        }
        System.out.println("ClientRequestHandler: Stopped processing for client " + clientAddress);
    }

    /**
     * Processes a received BaseClientRequest object by determining its specific type
     * and calling the appropriate handling method.
     *
     * @param request The BaseClientRequest object received from the client.
     */
    private void processRequest(BaseClientRequest request) {
        try {
            if (request instanceof FilterStoresClientRequest) {
                handleFilterStores((FilterStoresClientRequest) request);
            } else if (request instanceof BuyProductClientRequest) {
                handleBuyProduct((BuyProductClientRequest) request);
            }
            // Add else-if blocks here for other client request types
            // else if (request instanceof RateStoreClientRequest) { ... }
            else {
                System.err.println("ClientRequestHandler: Received valid but unhandled BaseClientRequest type: " + request.getClass().getName());
                // Optionally send an error response
                // sendResponse(new ErrorResponse("Unsupported request type."));
            }
        } catch (Exception e) {
            // Catch errors specific to request processing logic
            System.err.println("ClientRequestHandler: Error processing request (" + request.getClass().getSimpleName() + "): " + e.getMessage());
            e.printStackTrace();
            // Optionally send a generic error response to the client
            // sendResponse(new ErrorResponse("Internal server error processing request."));
        }
    }

    // --- Request Handling Methods ---

    /**
     * Handles FilterStoresClientRequest.
     * Calls the Master's MapReduce method and sends the results back to the client.
     * @param request The FilterStoresClientRequest from the client.
     */
    private void handleFilterStores(FilterStoresClientRequest request) {
        FilterCriteria criteria = request.getCriteria();
        System.out.println("ClientRequestHandler: Processing FILTER_STORES request.");

        // Validate criteria if necessary
        if (criteria == null) {
            System.err.println("ClientRequestHandler: Received null FilterCriteria in request.");
            sendResponse(new FilterResponse(null)); // Send empty/error response
            return;
        }

        // Call Master's method which handles MapReduce via Reducer
        List<String> results = master.performMapReduceFilterViaReducer(criteria);

        // Wrap results in response object (Master -> Client DTO) and send back
        FilterResponse response = new FilterResponse(results);
        sendResponse(response);
        System.out.println("ClientRequestHandler: Sent FilterResponse to client with " + (results != null ? results.size() : "null") + " results.");
    }

    /**
     * Handles BuyProductClientRequest.
     * Calls the Master's purchase method and sends the status back to the client.
     * @param request The BuyProductClientRequest from the client.
     */
    private void handleBuyProduct(BuyProductClientRequest request) {
        String storeName = request.getStoreName();
        String productName = request.getProductName();
        int quantity = request.getQuantity();
        System.out.println("ClientRequestHandler: Processing BUY_PRODUCT request for " + quantity + "x " + productName + " from " + storeName);

        // Basic validation
        if (storeName == null || storeName.trim().isEmpty() ||
                productName == null || productName.trim().isEmpty() ||
                quantity <= 0) {
            System.err.println("ClientRequestHandler: Invalid data in BuyProductClientRequest.");
            // Send back a failure response (using Worker->Master DTO status for simplicity, could define Client-specific ones)
            sendResponse(new BuyResponse(PurchaseResponse.Status.FAIL_WORKER_ERROR.toString())); // Or a more specific client error code
            return;
        }

        // Call Master's method which handles forwarding to worker and getting response
        PurchaseResponse purchaseWorkerResponse = master.performPurchase(storeName, productName, quantity);

        // Convert the Worker->Master response status to a simple status code string for the Client->Master response DTO
        String clientStatusCode = (purchaseWorkerResponse != null)
                ? purchaseWorkerResponse.toStatusCodeString()
                : PurchaseResponse.Status.FAIL_WORKER_ERROR.toString(); // Default to error if response is null

        // Wrap status code in response object (Master -> Client DTO) and send back
        BuyResponse response = new BuyResponse(clientStatusCode);
        sendResponse(response);
        System.out.println("ClientRequestHandler: Sent BuyResponse (Status code: " + clientStatusCode + ") to client.");
    }


    /**
     * Sends a Serializable response object back to the Client.
     * Synchronized on the outputStreamLock for thread safety.
     *
     * @param response The Serializable object to send (should ideally be a specific response DTO).
     */
    private void sendResponse(Serializable response) {
        // Ensure response is not null before sending
        if (response == null) {
            System.err.println("ClientRequestHandler: Attempted to send a null response to " + clientSocket.getInetAddress().getHostAddress());
            return;
        }
        try {
            // Synchronize writes to the output stream for this specific client
            synchronized (outputStreamLock) {
                System.out.println("ClientRequestHandler -> " + clientSocket.getInetAddress().getHostAddress() + ": Sending " + response.getClass().getSimpleName() + "...");
                objectOut.writeObject(response);
                objectOut.flush(); // Ensure data is sent
                objectOut.reset(); // Reset object stream state if sending multiple objects over time
            }
        } catch (IOException e) {
            System.err.println("ClientRequestHandler: Error sending response ("+ response.getClass().getSimpleName() +") to client " + clientSocket.getInetAddress().getHostAddress() + ": " + e.getMessage());
            // Attempt to close the connection if sending fails, as it's likely broken
            close();
        }
    }

    /**
     * Closes the client connection resources (streams and socket).
     * Called when the handler finishes or an unrecoverable error occurs.
     */
    @Override
    public void close() {
        // Avoid closing multiple times
        if (clientSocket == null || clientSocket.isClosed()) {
            return;
        }
        String clientAddress = clientSocket.getInetAddress().getHostAddress();
        System.out.println("ClientRequestHandler: Closing connection resources for client " + clientAddress);
        // Close streams first
        try { if (objectOut != null) objectOut.close(); } catch (IOException e) {/* ignore */}
        try { if (objectIn != null) objectIn.close(); } catch (IOException e) {/* ignore */}
        // Then close socket
        try { clientSocket.close(); }
        catch (IOException e) { System.err.println("ClientRequestHandler: Error closing client socket (" + clientAddress + "): " + e.getMessage()); }
    }
}
