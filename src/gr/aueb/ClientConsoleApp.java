package gr.aueb;

import gr.aueb.client.dtos.*;
import gr.aueb.client.dtos.PurchaseResponse;
import gr.aueb.model.FilterCriteria;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Scanner;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Dummy Client Console Application (CLI) for the Food Delivery System.
 * Uses Object Streams for Client <-> Master communication.
 * Sends request objects directly without an initial text identifier.
 */
public class ClientConsoleApp {

    private static final String DEFAULT_MASTER_IP = "127.0.0.1";
    private static final int DEFAULT_MASTER_PORT = 5000;

    private static Socket socket;
    // Use Object Streams directly
    private static ObjectOutputStream objectOut;
    private static ObjectInputStream objectIn;
    private static Scanner scanner;
    // Flag removed as response handling is now synchronous within the handler methods
    // private static volatile boolean waitingForFilterResults = false;
    private static final Object outputStreamLock = new Object(); // Lock for sending requests

    public static void main(String[] args) {
        String masterIp = DEFAULT_MASTER_IP;
        int masterPort = DEFAULT_MASTER_PORT;

        // --- Argument Parsing (same as before) ---
        if (args.length >= 2) { masterIp = args[0]; try { masterPort = Integer.parseInt(args[1]); } catch (NumberFormatException e) { System.err.println("Invalid Port format. Using default: " + DEFAULT_MASTER_PORT); } }
        else if (args.length == 1) { masterIp = args[0]; System.out.println("Using default Port: " + DEFAULT_MASTER_PORT); }
        else { System.out.println("Using default IP: " + DEFAULT_MASTER_IP + " and Port: " + DEFAULT_MASTER_PORT); }

        // Use try-with-resources for automatic closing of Scanner and Socket
        try (Scanner appScanner = new Scanner(System.in); // Renamed to avoid confusion with static field
             Socket clientSocket = new Socket(masterIp, masterPort)) { // Renamed

            socket = clientSocket; // Assign to static field if needed elsewhere (though ideally avoid static)
            scanner = appScanner; // Assign to static field

            System.out.println("Connected to Master: " + masterIp + ":" + masterPort);

            // --- Initialize Object Streams ---
            // IMPORTANT: Output stream MUST be initialized first
            objectOut = new ObjectOutputStream(socket.getOutputStream());
            objectIn = new ObjectInputStream(socket.getInputStream());
            System.out.println("Initialized Object Streams.");

            // --- Main Application Loop ---
            boolean running = true;
            while (running) {
                displayMenu();
                int choice = getUserChoice(scanner);

                try {
                    switch (choice) {
                        case 1:
                            handleFilterStores(); // Now blocks until response
                            break;
                        case 2:
                            handleBuyProduct(); // Now blocks until response
                            break;
                        case 3:
                            running = false;
                            System.out.println("Disconnecting...");
                            break;
                        default:
                            System.out.println("Invalid choice. Please try again.");
                    }
                } catch (IOException e) {
                    System.err.println("Communication error with Master: " + e.getMessage());
                    running = false; // Stop on communication error
                } catch (InputMismatchException e) {
                    System.err.println("Invalid input. Please enter a number.");
                    // Scanner state is handled in getUserChoice
                } catch (Exception e) {
                    System.err.println("An error occurred: " + e.getMessage());
                    e.printStackTrace(); // Print stack trace for debugging
                    // Consider if the app should terminate on unexpected errors
                }
            } // End while loop

        } catch (UnknownHostException e) {
            System.err.println("Master host not found: " + masterIp);
        } catch (IOException e) {
            System.err.println("Failed to connect or communicate with Master: " + masterIp + ":" + masterPort + " - " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error during startup: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeResources(); // Close streams
        }
        System.out.println("Client application terminated.");
    }

    /** Displays the main menu options */
    private static void displayMenu() {
        System.out.println("\n--- Client Menu (Object Stream) ---");
        System.out.println("1. Search Stores (with filters)");
        System.out.println("2. Buy Product");
        System.out.println("3. Exit");
        System.out.print("Choice: ");
    }

    /** Gets the user's menu choice */
    private static int getUserChoice(Scanner scanner) {
        try {
            int choice = scanner.nextInt();
            scanner.nextLine(); // Consume newline left-over
            return choice;
        } catch (InputMismatchException e) {
            System.err.println("Invalid input. Please enter an integer.");
            scanner.nextLine(); // Consume the invalid token and newline
            return -1; // Indicate invalid choice
        }
    }


    /**
     * Handles the "Filter Stores" action. Sends request and waits for/prints response.
     */
    private static void handleFilterStores() throws IOException {
        FilterCriteria criteria = getFilterCriteriaFromUser(scanner);
        if (criteria == null) return; // User input error

        FilterStoresClientRequest request = new FilterStoresClientRequest(criteria);
        System.out.println("Sending filter request...");
        sendRequestObject(request); // Send the request

        System.out.println("Filter request sent. Waiting for results...");

        // --- Wait for and process response ---
        try {
            Object responseObj = objectIn.readObject(); // Blocks here
            System.out.println("\n--- Search Results ---");
            if (responseObj instanceof FilterResponse) {
                FilterResponse response = (FilterResponse) responseObj;
                List<String> results = response.getStoreResultStrings();
                if (results == null || results.isEmpty()) {
                    System.out.println("No stores found matching the criteria.");
                } else {
                    System.out.println("Found " + results.size() + " store(s):");
                    // Basic parsing and printing - assumes specific format from Worker/Reducer
                    for (String resultLine : results) {
                        System.out.println("  " + resultLine); // Print raw string for now
                        // Example of more detailed parsing (adjust indices based on actual format):
                         /*
                         String[] parts = resultLine.split("\\|"); // Assuming pipe delimiter
                         if (parts.length >= 6) {
                             System.out.printf(" - Name: %s, Category: %s, Stars: %s, Price: %s (Lat: %s, Lon: %s)\n",
                                               parts[0], parts[3], parts[4], parts[5], parts[1], parts[2]);
                         } else {
                             System.out.println("   (Unrecognized format): " + resultLine);
                         }
                         */
                    }
                }
            } else {
                System.out.println("Received unexpected response type from Master: " +
                        (responseObj != null ? responseObj.getClass().getName() : "null"));
            }
            System.out.println("--------------------");

        } catch (ClassNotFoundException e) {
            System.err.println("Error: Could not find class for Master's response: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error reading response from Master: " + e.getMessage());
            throw e; // Re-throw IOExceptions to be caught by main loop
        }
    }

    /** Prompts the user to enter filter criteria */
    private static FilterCriteria getFilterCriteriaFromUser(Scanner scanner) {
        System.out.println("--- Enter Filter Criteria ---");
        // Default values
        double lat = 37.9838, lon = 23.7275, dist = 5.0;
        int stars = 1;
        String foodCatsStr = "", priceCatsStr = "";

        try {
            // Latitude
            System.out.print("Location - Latitude (e.g., " + lat + ", press Enter for default): ");
            String latStr = scanner.nextLine().trim();
            if (!latStr.isEmpty()) lat = Double.parseDouble(latStr);

            // Longitude
            System.out.print("Location - Longitude (e.g., " + lon + ", press Enter for default): ");
            String lonStr = scanner.nextLine().trim();
            if (!lonStr.isEmpty()) lon = Double.parseDouble(lonStr);

            // Max Distance
            System.out.print("Max Distance (km) (e.g., " + dist + ", press Enter for default): ");
            String distStr = scanner.nextLine().trim();
            if (!distStr.isEmpty()) dist = Double.parseDouble(distStr);
            if (dist < 0) { System.out.println("Distance cannot be negative. Using 0."); dist = 0; }


            // Food Categories
            System.out.print("Food Categories (e.g., pizza,burger - comma separated, blank for all): ");
            foodCatsStr = scanner.nextLine().trim();

            // Minimum Stars
            System.out.print("Minimum Stars (1-5) (e.g., " + stars + ", press Enter for default): ");
            String starsStr = scanner.nextLine().trim();
            if (!starsStr.isEmpty()) stars = Integer.parseInt(starsStr);
            if (stars < 1 || stars > 5) { System.out.println("Stars must be between 1 and 5. Using 1."); stars = 1; }

            // Price Categories
            System.out.print("Price Categories (e.g., $,$$ - comma separated, blank for all): ");
            priceCatsStr = scanner.nextLine().trim();

            // Parse comma-separated strings into lists
            List<String> foodCats = foodCatsStr.isEmpty() ? new ArrayList<>() :
                    Arrays.stream(foodCatsStr.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());

            List<String> priceCats = priceCatsStr.isEmpty() ? new ArrayList<>() :
                    Arrays.stream(priceCatsStr.split(","))
                            .map(String::trim)
                            .filter(s -> s.equals("$") || s.equals("$$") || s.equals("$$$")) // Validate price categories
                            .distinct() // Avoid duplicates
                            .collect(Collectors.toList());
            if (priceCats.size() != Arrays.stream(priceCatsStr.split(",")).map(String::trim).filter(s -> !s.isEmpty()).count()) {
                System.out.println("Warning: Invalid price categories ignored. Use only $, $$, $$$ separated by commas.");
            }


            return new FilterCriteria(lat, lon, dist, foodCats, stars, priceCats);

        } catch (NumberFormatException e) {
            System.err.println("Invalid numeric input: " + e.getMessage() + ". Please try again.");
            return null;
        } catch (Exception e) {
            System.err.println("Error reading criteria: " + e.getMessage() + ". Please try again.");
            return null;
        }
    }


    /**
     * Handles the "Buy Product" action. Sends request and waits for/prints response.
     */
    private static void handleBuyProduct() throws IOException {
        System.out.println("--- Buy Product ---");
        String storeName = "";
        String productName = "";
        int quantity = -1;

        try {
            System.out.print("Store Name: ");
            storeName = scanner.nextLine().trim();
            System.out.print("Product Name: ");
            productName = scanner.nextLine().trim();
            System.out.print("Quantity: ");
            quantity = scanner.nextInt();
            scanner.nextLine(); // Consume newline

            // Basic validation
            if (storeName.isEmpty() || productName.isEmpty()) {
                System.out.println("Store name and product name cannot be empty.");
                return;
            }
            if (quantity <= 0) {
                System.out.println("Quantity must be a positive number.");
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Invalid quantity. Please enter a whole number.");
            scanner.nextLine(); // Consume invalid input line
            return;
        }

        // Create and send request object
        BuyProductClientRequest request = new BuyProductClientRequest(storeName, productName, quantity);
        System.out.println("Sending buy request...");
        sendRequestObject(request);
        System.out.println("Buy request sent. Waiting for response...");

        // --- Wait for and process response ---
        try {
            Object responseObj = objectIn.readObject(); // Blocks here
            System.out.println("\n--- Purchase Result ---");
            if (responseObj instanceof BuyResponse) {
                BuyResponse response = (BuyResponse) responseObj;
                String statusCode = response.getStatusCode(); // Get status code string

                // Interpret status code based on PurchaseResponse.Status enum values
                if (statusCode == null) {
                    System.out.println("Received null status code from Master.");
                } else if (statusCode.equals(PurchaseResponse.Status.OK.toString())) { // Compare with enum string representation
                    System.out.println("Purchase successful!");
                } else if (statusCode.equals(PurchaseResponse.Status.FAIL_STOCK.toString())) {
                    System.out.println("Failed: Insufficient stock.");
                } else if (statusCode.equals(PurchaseResponse.Status.FAIL_PRODUCT_NOT_FOUND.toString())) {
                    System.out.println("Failed: Product not found in store.");
                } else if (statusCode.equals(PurchaseResponse.Status.FAIL_STORE_NOT_FOUND.toString())) {
                    System.out.println("Failed: Store not found (or worker unavailable).");
                } else if (statusCode.equals(PurchaseResponse.Status.FAIL_INVALID_QTY.toString())) {
                    System.out.println("Failed: Invalid quantity requested.");
                } else if (statusCode.equals(PurchaseResponse.Status.FAIL_WORKER_ERROR.toString())) {
                    System.out.println("Failed: An error occurred on the worker or during communication.");
                } else {
                    System.out.println("Unknown or unexpected status code from Master: " + statusCode);
                }
            } else {
                System.out.println("Received unexpected response type from Master: " +
                        (responseObj != null ? responseObj.getClass().getName() : "null"));
            }
            System.out.println("---------------------");
        } catch (ClassNotFoundException e) {
            System.err.println("Error: Could not find class for Master's response: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error reading response from Master: " + e.getMessage());
            throw e; // Re-throw IOExceptions
        }
    }

    /** Synchronized method to send a request object */
    private static void sendRequestObject(Serializable request) throws IOException {
        synchronized (outputStreamLock) {
            System.out.println("Sending request: " + request.getClass().getSimpleName());
            objectOut.writeObject(request);
            objectOut.flush();
            objectOut.reset(); // Good practice when reusing the stream
        }
    }

    /** Closes streams and socket */
    private static void closeResources() {
        System.out.println("Closing client resources...");
        try { if (scanner != null) scanner.close(); } catch (Exception e) {/* ignore */}
        // Close object streams first
        try { if (objectOut != null) objectOut.close(); } catch (IOException e) {/* ignore */}
        try { if (objectIn != null) objectIn.close(); } catch (IOException e) {/* ignore */}
        // Socket is closed by try-with-resources in main
        System.out.println("Client resources closed.");
    }

}