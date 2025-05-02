package gr.aueb;

import gr.aueb.manager.dtos.ManagerActionResponse; // Response from Master
import gr.aueb.manager.dtos.*; // Request DTOs
import gr.aueb.manager.dtos.ManagerActionResponse;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.InputMismatchException;
import java.util.Scanner;

/**
 * Manager Console Application (CLI) - Refactored for Object Streams
 * Allows managers to connect to the Master server and manage stores and products.
 * It communicates with the Master using TCP sockets and Object Streams, sending request objects.
 */
public class ManagerConsoleApp {

    private static final String DEFAULT_MASTER_IP = "127.0.0.1"; // Default Master IP address
    private static final int DEFAULT_MASTER_PORT = 5000;      // Default Master port

    // Object Streams for communication
    private static ObjectOutputStream objectOut;
    private static ObjectInputStream objectIn;
    private static final Object outputStreamLock = new Object(); // Lock for sending requests

    public static void main(String[] args) {
        String masterIp = DEFAULT_MASTER_IP;
        int masterPort = DEFAULT_MASTER_PORT;

        // --- Argument Parsing (same as before) ---
        if (args.length >= 2) {
            masterIp = args[0];
            try {
                masterPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid Port format. Using default: " + DEFAULT_MASTER_PORT);
            }
        } else if (args.length == 1) {
            masterIp = args[0];
            System.out.println("Using default Port: " + DEFAULT_MASTER_PORT);
        } else {
            System.out.println("Using default IP: " + DEFAULT_MASTER_IP + " and Port: " + DEFAULT_MASTER_PORT);
        }

        // Use try-with-resources for automatic closing of Scanner and Socket
        try (Scanner scanner = new Scanner(System.in);
             Socket socket = new Socket(masterIp, masterPort)) {

            System.out.println("Connected to Master: " + masterIp + ":" + masterPort);

            // --- Initialize Object Streams ---
            // IMPORTANT: Output stream MUST be initialized first
            objectOut = new ObjectOutputStream(socket.getOutputStream());
            objectIn = new ObjectInputStream(socket.getInputStream());
            System.out.println("Initialized Object Streams.");

            // --- Main Application Loop ---
            boolean running = true;
            while (running) {
                displayMenu(); // Show the options to the manager
                int choice = getUserChoice(scanner); // Get the manager's choice

                try {
                    switch (choice) {
                        case 1:
                            handleAddStore(scanner); // Pass only scanner
                            break;
                        case 2:
                            handleAddProduct(scanner); // Pass only scanner
                            break;
                        case 3:
                            handleRemoveProduct(scanner); // Pass only scanner
                            break;
                        case 4:
                            handleUpdateStock(scanner); // Pass only scanner
                            break;
                        case 5:
                            running = false; // Exit the loop
                            System.out.println("Disconnecting...");
                            break;
                        default:
                            System.out.println("Invalid choice. Please try again.");
                    }

                    // Wait for response from Master after sending a request (if applicable)
                    if (running && choice >= 1 && choice <= 4) {
                        waitForAndPrintResponse();
                    }

                } catch (IOException e) {
                    System.err.println("Communication error with Master: " + e.getMessage());
                    running = false; // Stop if communication fails
                } catch (InputMismatchException e) {
                    System.err.println("Invalid input. Please enter a number.");
                    scanner.next(); // Consume the invalid input
                } catch (Exception e) { // Catch other potential errors
                    System.err.println("An error occurred: " + e.getMessage());
                    e.printStackTrace(); // Print stack trace for debugging
                    // Consider whether to continue or exit based on the error
                }
            }

        } catch (UnknownHostException e) {
            System.err.println("Master host not found: " + masterIp);
        } catch (IOException e) {
            System.err.println("Failed to connect or communicate with Master: " + masterIp + ":" + masterPort);
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) { // Catch potential errors during initial setup
            System.err.println("Error during startup: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close streams if they were initialized (socket is closed by try-with-resources)
            closeResources();
        }

        System.out.println("Manager application terminated.");
    }

    /**
     * Displays the main menu options to the manager.
     */
    private static void displayMenu() {
        System.out.println("\n--- Manager Menu (Object Stream) ---");
        System.out.println("1. Add/Update Store (from JSON file)");
        System.out.println("2. Add Product to Store");
        System.out.println("3. Remove Product from Store");
        System.out.println("4. Update Product Stock");
        System.out.println("5. Exit");
        System.out.print("Choice: ");
    }

    /**
     * Gets the user's menu choice using the provided Scanner.
     * Handles potential InputMismatchException.
     *
     * @param scanner The Scanner object to read input.
     * @return The integer choice, or -1 if input is invalid.
     */
    private static int getUserChoice(Scanner scanner) {
        try {
            int choice = scanner.nextInt();
            scanner.nextLine(); // Consume the rest of the line (including newline)
            return choice;
        } catch (InputMismatchException e) {
            System.err.println("Invalid input. Please enter a number.");
            scanner.nextLine(); // Consume the invalid token and newline
            return -1; // Indicate invalid input
        }
    }

    /**
     * Sends a request object to the Master. Synchronized for safety.
     * @param request The request object (must be Serializable).
     * @throws IOException If an I/O error occurs during sending.
     */
    private static void sendRequestObject(Serializable request) throws IOException {
        synchronized (outputStreamLock) {
            System.out.println("Sending request: " + request.getClass().getSimpleName() + "...");
            objectOut.writeObject(request);
            objectOut.flush();
            objectOut.reset(); // Important for sending multiple objects over time
        }
    }

    /**
     * Waits for a response object from the Master and prints its details.
     */
    private static void waitForAndPrintResponse() {
        try {
            System.out.println("Waiting for response from Master...");
            Object responseObj = objectIn.readObject(); // Blocks here

            if (responseObj instanceof ManagerActionResponse) {
                ManagerActionResponse response = (ManagerActionResponse) responseObj;
                System.out.println("\n--- Master Response ---");
                System.out.println("Success: " + response.isSuccess());
                System.out.println("Message: " + response.getMessage());
                System.out.println("-----------------------");
            } else {
                System.err.println("\nReceived unexpected response type from Master: " +
                        (responseObj != null ? responseObj.getClass().getName() : "null"));
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("\nError receiving or reading response from Master: " + e.getMessage());
            // Consider closing the app or handling the error more robustly
        }
    }

    /**
     * Handles the "Add Store" action.
     * Prompts for JSON file path, reads the file, creates AddStoreManagerRequest,
     * and sends it to the Master.
     *
     * @param scanner Scanner for user input.
     * @throws IOException If file reading or socket communication fails.
     */
    private static void handleAddStore(Scanner scanner) throws IOException {
        System.out.print("Enter the path to the store's JSON file: ");
        String filePath = scanner.nextLine();

        try {
            // Read the entire JSON file content into a single string
            String jsonContent = new String(Files.readAllBytes(Paths.get(filePath)));

            // Create the request object
            AddStoreManagerRequest request = new AddStoreManagerRequest(jsonContent);

            // Send the request object to the Master
            sendRequestObject(request);
            System.out.println("Add/Update Store request sent.");

        } catch (IOException e) {
            System.err.println("Error reading file '" + filePath + "': " + e.getMessage());
        } catch (InvalidPathException e) {
            System.err.println("Invalid file path: " + filePath);
        } catch (SecurityException e) {
            System.err.println("Permission denied reading file: " + filePath);
        }
    }

    /**
     * Handles the "Add Product" action.
     * Prompts for store name and product details, creates AddProductManagerRequest,
     * and sends it to the Master.
     *
     * @param scanner Scanner for user input.
     * @throws IOException            If socket communication fails.
     * @throws InputMismatchException If numeric input is invalid.
     */
    private static void handleAddProduct(Scanner scanner) throws IOException, InputMismatchException {
        try {
            System.out.print("Store Name: ");
            String storeName = scanner.nextLine();
            System.out.print("New Product Name: ");
            String productName = scanner.nextLine();
            System.out.print("New Product Type (e.g., pizza, salad): ");
            String productType = scanner.nextLine();
            System.out.print("Available Amount: ");
            int amount = scanner.nextInt();
            System.out.print("Product Price: ");
            double price = scanner.nextDouble();
            scanner.nextLine(); // Consume newline

            // Basic validation
            if (storeName.trim().isEmpty() || productName.trim().isEmpty() || productType.trim().isEmpty() || amount < 0 || price < 0) {
                System.out.println("Invalid input. Please check store/product names, amount, and price.");
                return;
            }

            // Create the request object
            AddProductManagerRequest request = new AddProductManagerRequest(storeName, productName, productType, amount, price);

            // Send the request object
            sendRequestObject(request);
            System.out.println("Add Product request sent.");

        } catch (InputMismatchException e) {
            System.err.println("Invalid numeric input for amount or price.");
            scanner.nextLine(); // Consume the invalid input line
        }
    }

    /**
     * Handles the "Remove Product" action.
     * Prompts for store and product name, creates RemoveProductManagerRequest,
     * and sends it to the Master.
     *
     * @param scanner Scanner for user input.
     * @throws IOException If socket communication fails.
     */
    private static void handleRemoveProduct(Scanner scanner) throws IOException {
        System.out.print("Store Name: ");
        String storeName = scanner.nextLine();
        System.out.print("Product Name to Remove: ");
        String productName = scanner.nextLine();

        // Basic validation
        if (storeName.trim().isEmpty() || productName.trim().isEmpty()) {
            System.out.println("Store name and product name cannot be empty.");
            return;
        }

        // Create the request object
        RemoveProductManagerRequest request = new RemoveProductManagerRequest(storeName, productName);

        // Send the request object
        sendRequestObject(request);
        System.out.println("Remove Product request sent.");
    }

    /**
     * Handles the "Update Stock" action.
     * Prompts for store, product, and new amount, creates UpdateStockManagerRequest,
     * and sends it to the Master.
     *
     * @param scanner Scanner for user input.
     * @throws IOException            If socket communication fails.
     * @throws InputMismatchException If numeric input is invalid.
     */
    private static void handleUpdateStock(Scanner scanner) throws IOException, InputMismatchException {
        try {
            System.out.print("Store Name: ");
            String storeName = scanner.nextLine();
            System.out.print("Product Name: ");
            String productName = scanner.nextLine();
            System.out.print("New Available Amount: ");
            int newAmount = scanner.nextInt();
            scanner.nextLine(); // Consume newline

            // Basic validation
            if (storeName.trim().isEmpty() || productName.trim().isEmpty() || newAmount < 0) {
                System.out.println("Invalid input. Please check store/product names and amount (must be >= 0).");
                return;
            }

            // Create the request object
            UpdateStockManagerRequest request = new UpdateStockManagerRequest(storeName, productName, newAmount);

            // Send the request object
            sendRequestObject(request);
            System.out.println("Update Stock request sent.");

        } catch (InputMismatchException e) {
            System.err.println("Invalid numeric input for amount.");
            scanner.nextLine(); // Consume the invalid input line
        }
    }

    /**
     * Closes the communication streams.
     */
    private static void closeResources() {
        System.out.println("Closing manager resources...");
        try { if (objectOut != null) objectOut.close(); } catch (IOException e) {/* ignore */}
        try { if (objectIn != null) objectIn.close(); } catch (IOException e) {/* ignore */}
        // Socket is closed by try-with-resources in main
    }
}