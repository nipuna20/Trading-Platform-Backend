package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/rs/cors"
)

type Order struct {
	ID                 int            `json:"id"`
	UserID             int            `json:"user_id"`
	TransactionID      string         `json:"transaction_id"`
	Role               string         `json:"role"`
	Price              float64        `json:"price"`
	Quantity           int            `json:"quantity"`
	TradeDate          string         `json:"trade_date"`
	TradeTime          string         `json:"trade_time"`
	TransactionType    int            `json:"transaction_type"`
	MatchType          int            `json:"match_type"`
	MarketLeadProgram  bool           `json:"market_lead_program"`
	ProjectID          *int           `json:"project_id"`
	CreatedAt          time.Time      `json:"created_at"`
}

type BuyerOrderHistory struct {
	ID                 int       `json:"id"`
	BuyerOrderID       int       `json:"buyer_order_id"`
	BuyerUserID        int       `json:"buyer_user_id"`
	BuyerTransactionID string    `json:"buyer_transaction_id"`
	OriginalPrice      float64   `json:"original_price"`
	OriginalQty        int       `json:"original_qty"`
	BuyerTradeDate     string    `json:"buyer_trade_date"`
	BuyerTradeTime     string    `json:"buyer_trade_time"`
	ProjectID          int       `json:"project_id"`
	TotalMatchedQty    int       `json:"total_matched_qty"`
	RemainingQty       int       `json:"remaining_qty"`
	MatchCount         int       `json:"match_count"`
	SellerCount        int       `json:"seller_count"`
	Status             string    `json:"status"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

var db *sql.DB

// Global matching engine control
var (
	matchingEnabled      = true
	matchingEnabledMutex sync.RWMutex
)

func initDB() {
	var err error
	
	databaseURL := os.Getenv("DATABASE_URL")
	
	var connStr string
	if databaseURL != "" {
		// Manual parsing to handle special characters
		// Format: postgresql://user:password@host:port/database
		
		// Remove the scheme
		urlWithoutScheme := strings.TrimPrefix(databaseURL, "postgresql://")
		urlWithoutScheme = strings.TrimPrefix(urlWithoutScheme, "postgres://")
		
		// Split by @ to separate credentials from host
		parts := strings.Split(urlWithoutScheme, "@")
		if len(parts) != 2 {
			log.Fatal("Invalid DATABASE_URL format")
		}
		
		credentials := parts[0]
		hostAndDB := parts[1]
		
		// Split credentials into username and password
		credParts := strings.SplitN(credentials, ":", 2)
		if len(credParts) != 2 {
			log.Fatal("Invalid DATABASE_URL credentials format")
		}
		username := credParts[0]
		password := credParts[1]
		
		// Split host:port/database
		hostParts := strings.Split(hostAndDB, "/")
		if len(hostParts) != 2 {
			log.Fatal("Invalid DATABASE_URL host format")
		}
		hostPort := hostParts[0]
		dbname := hostParts[1]
		
		// Split host and port
		hostPortParts := strings.Split(hostPort, ":")
		host := hostPortParts[0]
		port := "5432"
		if len(hostPortParts) == 2 {
			port = hostPortParts[1]
		}
		
		// Build connection string in key=value format (lib/pq format)
		connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
			host,
			port,
			username,
			password,
			dbname,
		)
		
		log.Println("Using DATABASE_URL from environment")
		log.Printf("Connecting to: postgres://%s:***@%s:%s/%s", username, host, port, dbname)
	} else {
		// Fallback to individual env vars for local development
		connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			getEnv("DB_HOST", "localhost"),
			getEnv("DB_PORT", "5432"),
			getEnv("DB_USER", "postgres"),
			getEnv("DB_PASSWORD", "admin"),
			getEnv("DB_NAME", "trading_platform"),
		)
		log.Println("Using individual DB env vars (local development)")
	}
	
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error connecting to database:", err)
	}
	
	// Set connection pool settings for Railway
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	if err = db.Ping(); err != nil {
		log.Fatal("Error pinging database:", err)
	}
	
	log.Println("✅ Successfully connected to database")
	
	createAuthTables(db)      
    createProjectsTable()     
    createTables()
	addAdminColumn(db)
	initTopOrdersTables(db)
	initMatchedOrdersTable(db)
	initBuyerOrderHistoryTable(db)
	initMatchAssignmentsTable(db)
	initCircuitBreakerTable(db)
	
	cleanupNullProjectIds()
	
	if err := syncTopOrdersIfEmpty(db); err != nil {
		log.Println("Warning: Error during initial top orders sync:", err)
	}
	
	if err := matchAllOrders(db); err != nil {
		log.Println("Warning: Error during initial matching:", err)
	}
}

func cleanupNullProjectIds() {
	queries := []string{
		`UPDATE buyer SET project_id = 1 WHERE project_id IS NULL`,
		`UPDATE seller SET project_id = 1 WHERE project_id IS NULL`,
		`UPDATE top_buyer SET project_id = 1 WHERE project_id IS NULL`,
		`UPDATE top_seller SET project_id = 1 WHERE project_id IS NULL`,
	}
	
	for _, query := range queries {
		result, err := db.Exec(query)
		if err != nil {
			log.Printf("Warning: Error cleaning up NULL project_ids: %v", err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				log.Printf("✅ Updated %d rows with default project_id = 1", rowsAffected)
			}
		}
	}
}

func createProjectsTable() {
	query := `CREATE TABLE IF NOT EXISTS projects (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL UNIQUE,
		description TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	
	_, err := db.Exec(query)
	if err != nil {
		log.Fatal("Error creating projects table:", err)
	}
	
	insertQuery := `
		INSERT INTO projects (name, description) VALUES
		($1, $2)
		ON CONFLICT (name) DO NOTHING
	`
	
	projects := []struct {
		name string
		desc string
	}{
		{"Real Estate Development", "Commercial and residential property projects"},
		{"Green Energy", "Solar and wind power initiatives"},
		{"Tech Startup Fund", "Early-stage technology companies"},
		{"Healthcare Innovation", "Medical technology and services"},
		{"E-commerce Platform", "Online retail and marketplace"},
		{"Financial Services", "Fintech and banking solutions"},
		{"Education Technology", "Digital learning platforms"},
		{"Agriculture Tech", "Smart farming and agritech"},
		{"Manufacturing", "Industrial production facilities"},
		{"Logistics & Supply Chain", "Transportation and warehousing"},
		{"Tourism & Hospitality", "Hotels and travel services"},
		{"Entertainment Media", "Content creation and streaming"},
		{"Biotechnology", "Pharmaceutical and life sciences"},
		{"Cybersecurity", "Data protection and security"},
		{"Artificial Intelligence", "AI research and applications"},
		{"Blockchain Projects", "Cryptocurrency and DeFi"},
		{"Automotive Industry", "Vehicle manufacturing and EV"},
		{"Space Technology", "Aerospace and satellite"},
		{"Food & Beverage", "Restaurant and food processing"},
		{"Telecommunications", "Network infrastructure and 5G"},
	}
	
	for _, p := range projects {
		_, err := db.Exec(insertQuery, p.name, p.desc)
		if err != nil {
			log.Printf("Warning: Could not insert project %s: %v", p.name, err)
		}
	}
	
	log.Println("✅ Projects table created with 20 default projects")
}

func createTables() {
	_, err := db.Exec(`CREATE SEQUENCE IF NOT EXISTS transaction_seq START 10000000;`)
	if err != nil {
		log.Fatal("Error creating transaction sequence:", err)
	}

	tables := []string{
		`CREATE TABLE IF NOT EXISTS buyer (
			id SERIAL PRIMARY KEY,
			transaction_id VARCHAR(8) UNIQUE NOT NULL DEFAULT LPAD(nextval('transaction_seq')::text, 8, '0'),
			user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
			price DECIMAL(10, 2) NOT NULL,
			quantity INTEGER NOT NULL,
			trade_date DATE NOT NULL,
			trade_time TIME NOT NULL,
			transaction_type INTEGER NOT NULL CHECK (transaction_type IN (0, 1, 2)),
			match_type INTEGER NOT NULL DEFAULT 0 CHECK (match_type IN (0, 1)),
			market_lead_program BOOLEAN NOT NULL DEFAULT false,
			project_id INTEGER DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS seller (
			id SERIAL PRIMARY KEY,
			transaction_id VARCHAR(8) UNIQUE NOT NULL DEFAULT LPAD(nextval('transaction_seq')::text, 8, '0'),
			user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
			price DECIMAL(10, 2) NOT NULL,
			quantity INTEGER NOT NULL,
			trade_date DATE NOT NULL,
			trade_time TIME NOT NULL,
			transaction_type INTEGER NOT NULL CHECK (transaction_type IN (0, 1, 2)),
			match_type INTEGER NOT NULL DEFAULT 0 CHECK (match_type IN (0, 1)),
			market_lead_program BOOLEAN NOT NULL DEFAULT false,
			project_id INTEGER DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, table := range tables {
		_, err := db.Exec(table)
		if err != nil {
			log.Fatal("Error creating table:", err)
		}
	}

	alterQueries := []string{
		`ALTER TABLE buyer ADD COLUMN IF NOT EXISTS match_type INTEGER NOT NULL DEFAULT 0 CHECK (match_type IN (0, 1))`,
		`ALTER TABLE buyer ADD COLUMN IF NOT EXISTS market_lead_program BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE buyer ADD COLUMN IF NOT EXISTS project_id INTEGER DEFAULT 1`,
		`ALTER TABLE seller ADD COLUMN IF NOT EXISTS match_type INTEGER NOT NULL DEFAULT 0 CHECK (match_type IN (0, 1))`,
		`ALTER TABLE seller ADD COLUMN IF NOT EXISTS market_lead_program BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE seller ADD COLUMN IF NOT EXISTS project_id INTEGER DEFAULT 1`,
	}

	for _, query := range alterQueries {
		_, err := db.Exec(query)
		if err != nil {
			log.Printf("Warning executing alter query: %v", err)
		}
	}

	log.Println("All tables created/updated with project_id field")
}

func getProjects(w http.ResponseWriter, r *http.Request) {
	query := `SELECT id, name, description FROM projects ORDER BY name ASC`
	
	rows, err := db.Query(query)
	if err != nil {
		log.Println("Error querying projects:", err)
		http.Error(w, "Error fetching projects", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	
	type Project struct {
		ID          int    `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	
	projects := []Project{}
	for rows.Next() {
		var p Project
		err := rows.Scan(&p.ID, &p.Name, &p.Description)
		if err != nil {
			log.Println("Error scanning project:", err)
			continue
		}
		projects = append(projects, p)
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(projects)
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	var order Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if order.Role == "" || order.UserID == 0 || order.Price == 0 || order.Quantity == 0 || 
	   order.TradeDate == "" || order.TradeTime == "" || order.ProjectID == nil || *order.ProjectID == 0 {
		http.Error(w, "All fields including project_id are required", http.StatusBadRequest)
		return
	}

	if order.TransactionType < 0 || order.TransactionType > 2 {
		http.Error(w, "Invalid transaction type", http.StatusBadRequest)
		return
	}

	if order.MatchType < 0 || order.MatchType > 1 {
		http.Error(w, "Invalid match type", http.StatusBadRequest)
		return
	}

	if len(order.TradeDate) != 10 {
		http.Error(w, "Invalid trade_date format", http.StatusBadRequest)
		return
	}

	if len(order.TradeTime) > 8 {
		if idx := strings.Index(order.TradeTime, "T"); idx != -1 {
			order.TradeTime = order.TradeTime[idx+1:]
		}
		order.TradeTime = strings.Split(order.TradeTime, "Z")[0]
		order.TradeTime = strings.Split(order.TradeTime, "+")[0]
	}

	if len(order.TradeTime) == 5 && order.TradeTime[2] == ':' {
		order.TradeTime = order.TradeTime + ":00"
	}

	tableName := getTableName(order.Role)
	if tableName == "" {
		http.Error(w, "Invalid role", http.StatusBadRequest)
		return
	}

	// FIX: Pass by reference (&order) so 'order' struct gets the new ID
	err = intelligentOrderInsertion(db, &order)
	if err != nil {
		log.Println("Error inserting order:", err)
		http.Error(w, "Error creating order", http.StatusInternalServerError)
		return
	}

	// Now order.ID is correctly set (e.g., 170)
	if order.Role == "buyer" {
		if err := recordBuyerOrderHistory(db, order); err != nil {
			log.Printf("⚠️ Warning: Could not record buyer order history: %v", err)
		}
	}

	if err := checkAndTriggerMatching(db); err != nil {
		log.Println("Warning: Error during matching check:", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}

// NEW: Manual Cancel/Reject Order Handler
func cancelOrder(w http.ResponseWriter, r *http.Request) {
	// 1. Authorization Check
	token := r.Header.Get("Authorization")
	if token == "" {
		http.Error(w, "Unauthorized: No token provided", http.StatusUnauthorized)
		return
	}
	
	requesterID, err := getUserIDFromToken(token, db)
	if err != nil {
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	// 2. Parse Request
	vars := mux.Vars(r)
	role := vars["role"]
	idStr := vars["id"]
	orderID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid order ID", http.StatusBadRequest)
		return
	}

	if role != "buyer" && role != "seller" {
		http.Error(w, "Invalid role", http.StatusBadRequest)
		return
	}

	// 3. Find Order and Verify Ownership/Admin
	topTable := "top_" + role
	mainTable := role
	var ownerID int
	var inTopTable bool

	// Check Top Table First
	err = db.QueryRow("SELECT user_id FROM "+topTable+" WHERE order_id = $1", orderID).Scan(&ownerID)
	if err == nil {
		inTopTable = true
	} else {
		// If not in top, check Main Table
		err = db.QueryRow("SELECT user_id FROM "+mainTable+" WHERE id = $1", orderID).Scan(&ownerID)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Order not found", http.StatusNotFound)
			} else {
				http.Error(w, "Database error", http.StatusInternalServerError)
			}
			return
		}
		inTopTable = false
	}

	// Check if Requester is Owner or Admin
	if requesterID != ownerID && !isAdmin(requesterID, db) {
		http.Error(w, "Forbidden: You can only cancel your own orders", http.StatusForbidden)
		return
	}

	// 4. Execute Cancellation
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Transaction error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	if inTopTable {
		_, err = tx.Exec("DELETE FROM "+topTable+" WHERE order_id = $1", orderID)
	} else {
		_, err = tx.Exec("DELETE FROM "+mainTable+" WHERE id = $1", orderID)
	}

	if err != nil {
		log.Printf("Error deleting order %d: %v", orderID, err)
		http.Error(w, "Failed to cancel order", http.StatusInternalServerError)
		return
	}

	// Update History Status (Only for Buyers)
	if role == "buyer" {
		_, err = tx.Exec(`
			UPDATE buyer_order_history 
			SET status = 'Cancelled', updated_at = CURRENT_TIMESTAMP 
			WHERE buyer_order_id = $1
		`, orderID)
		if err != nil {
			log.Printf("Warning: Failed to update history for cancelled order %d: %v", orderID, err)
		}
	}

	if err = tx.Commit(); err != nil {
		http.Error(w, "Commit error", http.StatusInternalServerError)
		return
	}

	// 5. Post-Cancellation Sync (Refill Top Table if needed)
	if inTopTable {
		go func() {
			log.Printf("🔄 Order #%d cancelled from TOP table. Syncing...", orderID)
			if err := syncTopOrders(db, role); err != nil {
				log.Printf("Error syncing top orders after cancellation: %v", err)
			}
		}()
	}

	log.Printf("🗑️ Order #%d (%s) cancelled by User %d", orderID, role, requesterID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Order cancelled successfully",
		"id":      orderID,
	})
}

func getOrders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	role := vars["role"]
	transactionTypeStr := vars["transaction_type"]

	tableName := getTableName(role)
	if tableName == "" {
		http.Error(w, "Invalid role", http.StatusBadRequest)
		return
	}

	var orderByClause string
	if role == "buyer" {
		orderByClause = "ORDER BY price DESC, quantity DESC, trade_date ASC, trade_time ASC, created_at DESC"
	} else {
		orderByClause = "ORDER BY price ASC, quantity DESC, trade_date ASC, trade_time ASC, created_at DESC"
	}

	var query string
	var rows *sql.Rows
	var err error

	selectFields := `id, transaction_id, user_id, price, quantity, trade_date, 
		TO_CHAR(trade_time, 'HH24:MI:SS') as trade_time, transaction_type, match_type, market_lead_program, 
		COALESCE(project_id, 1) as project_id, created_at`

	if transactionTypeStr == "all" {
		query = fmt.Sprintf(`SELECT %s FROM %s %s`, selectFields, tableName, orderByClause)
		rows, err = db.Query(query)
	} else {
		var transactionType int
		fmt.Sscanf(transactionTypeStr, "%d", &transactionType)
		
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE transaction_type = $1 %s`, 
			selectFields, tableName, orderByClause)
		rows, err = db.Query(query, transactionType)
	}

	if err != nil {
		log.Println("Error querying orders:", err)
		http.Error(w, "Error fetching orders", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	orders := []Order{}
	for rows.Next() {
		var order Order
		var projectID int
		err := rows.Scan(&order.ID, &order.TransactionID, &order.UserID, &order.Price, &order.Quantity, 
			&order.TradeDate, &order.TradeTime, &order.TransactionType, &order.MatchType, 
			&order.MarketLeadProgram, &projectID, &order.CreatedAt)
		if err != nil {
			log.Println("Error scanning row:", err)
			continue
		}
		order.ProjectID = &projectID
		order.Role = role
		orders = append(orders, order)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}

func getAllOrders(w http.ResponseWriter, r *http.Request) {
	tables := []struct {
		name string
		role string
	}{
		{"buyer", "buyer"},
		{"seller", "seller"},
	}

	allOrders := make(map[string][]Order)

	for _, t := range tables {
		var orderByClause string
		if t.role == "buyer" {
			orderByClause = "ORDER BY price DESC, quantity DESC, trade_date ASC, trade_time ASC, created_at DESC"
		} else {
			orderByClause = "ORDER BY price ASC, quantity DESC, trade_date ASC, trade_time ASC, created_at DESC"
		}

		selectFields := `id, transaction_id, user_id, price, quantity, trade_date, 
			TO_CHAR(trade_time, 'HH24:MI:SS') as trade_time, transaction_type, match_type, market_lead_program, 
			COALESCE(project_id, 1) as project_id, created_at`

		query := fmt.Sprintf(`SELECT %s FROM %s %s`, selectFields, t.name, orderByClause)

		rows, err := db.Query(query)
		if err != nil {
			log.Println("Error querying", t.name, ":", err)
			continue
		}

		orders := []Order{}
		for rows.Next() {
			var order Order
			var projectID int
			err := rows.Scan(&order.ID, &order.TransactionID, &order.UserID, &order.Price, &order.Quantity,
				&order.TradeDate, &order.TradeTime, &order.TransactionType, &order.MatchType, 
				&order.MarketLeadProgram, &projectID, &order.CreatedAt)
			if err != nil {
				log.Println("Error scanning row:", err)
				continue
			}
			order.ProjectID = &projectID
			order.Role = t.role
			orders = append(orders, order)
		}
		rows.Close()

		allOrders[t.name] = orders
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(allOrders)
}

func getTopOrders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	role := vars["role"]
	transactionTypeStr := vars["transaction_type"]

	var transactionType int
	fmt.Sscanf(transactionTypeStr, "%d", &transactionType)

	orders, err := getTopOrdersData(db, role, transactionType)
	if err != nil {
		log.Println("Error fetching top orders:", err)
		http.Error(w, "Error fetching top orders", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}

func getAllTopOrders(w http.ResponseWriter, r *http.Request) {
	configs := []struct {
		role            string
		transactionType int
	}{
		{"buyer", 0},
		{"buyer", 1},
		{"buyer", 2},
		{"seller", 0},
		{"seller", 1},
		{"seller", 2},
	}

	allTopOrders := make(map[string][]Order)

	for _, config := range configs {
		orders, err := getTopOrdersData(db, config.role, config.transactionType)
		if err != nil {
			log.Println("Error querying top orders for", config.role, config.transactionType, ":", err)
			continue
		}

		key := fmt.Sprintf("top_%s_%d", config.role, config.transactionType)
		allTopOrders[key] = orders
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(allTopOrders)
}

func getMatchedOrders(w http.ResponseWriter, r *http.Request) {
	matches, err := getMatchedOrdersData(db)
	if err != nil {
		log.Println("Error fetching matched orders:", err)
		http.Error(w, "Error fetching matched orders", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matches)
}

func getUserMatchedOrders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userIDStr := vars["user_id"]
	
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		http.Error(w, "Invalid user ID", http.StatusBadRequest)
		return
	}

	matches, err := getMatchedOrdersByUser(db, userID)
	if err != nil {
		log.Println("Error fetching user matched orders:", err)
		http.Error(w, "Error fetching matched orders", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matches)
}

func getBuyerOrderHistoryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	buyerIDStr := vars["buyer_id"]

	buyerID, err := strconv.Atoi(buyerIDStr)
	if err != nil {
		http.Error(w, "Invalid buyer ID", http.StatusBadRequest)
		return
	}

	history, err := getBuyerOrderHistory(db, buyerID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Buyer order not found", http.StatusNotFound)
		} else {
			log.Println("Error fetching buyer order history:", err)
			http.Error(w, "Error fetching buyer order history", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
}

func getMatchAssignmentsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	buyerIDStr := vars["buyer_id"]

	buyerID, err := strconv.Atoi(buyerIDStr)
	if err != nil {
		http.Error(w, "Invalid buyer ID", http.StatusBadRequest)
		return
	}

	assignments, err := getMatchAssignments(db, buyerID)
	if err != nil {
		log.Println("Error fetching match assignments:", err)
		http.Error(w, "Error fetching match assignments", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(assignments)
}

func getUnmatchedBuyerOrdersHandler(w http.ResponseWriter, r *http.Request) {
	query := `
		SELECT id, buyer_order_id, buyer_user_id, buyer_transaction_id, original_price, original_qty,
		       buyer_trade_date, TO_CHAR(buyer_trade_time, 'HH24:MI:SS'), project_id, 
		       total_matched_qty, remaining_qty, match_count, seller_count, status, created_at, updated_at
		FROM buyer_order_history
		WHERE status IN ('Pending', 'Partially Matched')
		ORDER BY updated_at DESC
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Println("Error fetching unmatched orders:", err)
		http.Error(w, "Error fetching unmatched orders", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	histories := []BuyerOrderHistory{}
	for rows.Next() {
		var h BuyerOrderHistory
		var tradeTime string
		err := rows.Scan(&h.ID, &h.BuyerOrderID, &h.BuyerUserID, &h.BuyerTransactionID,
			&h.OriginalPrice, &h.OriginalQty, &h.BuyerTradeDate, &tradeTime,
			&h.ProjectID, &h.TotalMatchedQty, &h.RemainingQty, &h.MatchCount,
			&h.SellerCount, &h.Status, &h.CreatedAt, &h.UpdatedAt)
		if err != nil {
			log.Println("Error scanning row:", err)
			continue
		}
		h.BuyerTradeTime = tradeTime
		histories = append(histories, h)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(histories)
}

func triggerMatching(w http.ResponseWriter, r *http.Request) {
	matchStart := time.Now()
	
	if err := matchAllOrders(db); err != nil {
		log.Println("Error during manual matching:", err)
		http.Error(w, "Error during matching", http.StatusInternalServerError)
		return
	}
	
	duration := time.Since(matchStart)
	
	response := map[string]interface{}{
		"status":       "success",
		"message":      "Matching completed",
		"duration_ms":  float64(duration.Microseconds()) / 1000.0,
		"duration_str": duration.String(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Clear all data from tables
func clearAllData(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if token == "" {
		http.Error(w, "Unauthorized: No token provided", http.StatusUnauthorized)
		return
	}
	
	userID, err := getUserIDFromToken(token, db)
	if err != nil {
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}
	
	if !isAdmin(userID, db) {
		http.Error(w, "Forbidden: Admin access required", http.StatusForbidden)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Error starting transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	tables := []string{
		"match_assignments",
		"matched_orders",
		"buyer_order_history",
		"top_buyer",
		"top_seller",
		"buyer",
		"seller",
		// "sessions" removed so users stay logged in
		"project_circuit_breakers",
	}

	deletedCounts := make(map[string]int64)

	for _, table := range tables {
		result, err := tx.Exec(fmt.Sprintf("DELETE FROM %s", table))
		if err != nil {
			log.Printf("Error clearing %s: %v", table, err)
			http.Error(w, fmt.Sprintf("Error clearing %s", table), http.StatusInternalServerError)
			return
		}
		count, _ := result.RowsAffected()
		deletedCounts[table] = count
	}

	_, err = tx.Exec("ALTER SEQUENCE transaction_seq RESTART WITH 10000000")
	if err != nil {
		log.Printf("Warning: Could not reset transaction sequence: %v", err)
	}

	if err = tx.Commit(); err != nil {
		http.Error(w, "Error committing transaction", http.StatusInternalServerError)
		return
	}

	log.Printf("🗑️  DATABASE CLEARED by admin (User ID: %d)", userID)
	for table, count := range deletedCounts {
		if count > 0 {
			log.Printf("   - %s: %d rows deleted", table, count)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":        true,
		"message":        "All trading data cleared successfully",
		"deleted_counts": deletedCounts,
	})
}

// Toggle matching engine
func toggleMatchingEngine(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if token == "" {
		http.Error(w, "Unauthorized: No token provided", http.StatusUnauthorized)
		return
	}
	
	userID, err := getUserIDFromToken(token, db)
	if err != nil {
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}
	
	if !isAdmin(userID, db) {
		http.Error(w, "Forbidden: Admin access required", http.StatusForbidden)
		return
	}

	var req struct {
		Enabled bool `json:"enabled"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	matchingEnabledMutex.Lock()
	matchingEnabled = req.Enabled
	matchingEnabledMutex.Unlock()

	status := "STOPPED"
	if req.Enabled {
		status = "STARTED"
	}

	log.Printf("⚙️  MATCHING ENGINE %s by admin (User ID: %d)", status, userID)

	// NEW: If enabling matching engine, check if there are orders to match
	if req.Enabled {
		go func() {
			log.Println("🔍 Checking top tables for pending matches...")
			
			var buyerCount, sellerCount int
			err := db.QueryRow("SELECT COUNT(*) FROM top_buyer").Scan(&buyerCount)
			if err != nil {
				log.Printf("⚠️ Error counting buyers: %v", err)
				return
			}

			err = db.QueryRow("SELECT COUNT(*) FROM top_seller").Scan(&sellerCount)
			if err != nil {
				log.Printf("⚠️ Error counting sellers: %v", err)
				return
			}

			log.Printf("📊 Top tables status - Buyers: %d, Sellers: %d", buyerCount, sellerCount)

			// If both tables have orders, start matching
			if buyerCount >= 1 && sellerCount >= 1 {
				log.Println("✅ Both tables have orders - Auto-starting matching process...")
				
				// Check circuit breakers first
				if err := checkAndUpdateCircuitBreakers(db); err != nil {
					log.Printf("⚠️ Warning: Circuit breaker check failed: %v", err)
				}

				matchStart := time.Now()
				if err := matchAllOrders(db); err != nil {
					log.Printf("❌ Matching error: %v", err)
					return
				}

				duration := time.Since(matchStart)
				durationMs := float64(duration.Microseconds()) / 1000.0
				log.Printf("⚡ Auto-matching completed in %.3fms", durationMs)
			} else {
				log.Printf("⏳ Not enough orders to match - Waiting for more orders (Buyers: %d, Sellers: %d)", buyerCount, sellerCount)
			}
		}()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"enabled": matchingEnabled,
		"message": fmt.Sprintf("Matching engine %s", strings.ToLower(status)),
	})
}

// Get matching engine status
func getMatchingStatus(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if token == "" {
		http.Error(w, "Unauthorized: No token provided", http.StatusUnauthorized)
		return
	}
	
	userID, err := getUserIDFromToken(token, db)
	if err != nil {
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}
	
	if !isAdmin(userID, db) {
		http.Error(w, "Forbidden: Admin access required", http.StatusForbidden)
		return
	}

	matchingEnabledMutex.RLock()
	enabled := matchingEnabled
	matchingEnabledMutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled": enabled,
	})
}

func getTableName(role string) string {
	switch role {
	case "buyer":
		return "buyer"
	case "seller":
		return "seller"
	}
	return ""
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func main() {
	initDB()
	defer db.Close()

	router := mux.NewRouter()

	router.HandleFunc("/health", healthCheck).Methods("GET")

	// AUTHENTICATION ROUTES
	router.HandleFunc("/api/auth/register", registerHandler).Methods("POST")
	router.HandleFunc("/api/auth/login", loginHandler).Methods("POST")
	router.HandleFunc("/api/auth/logout", logoutHandler).Methods("POST")
	router.HandleFunc("/api/auth/verify", verifyTokenHandler).Methods("GET")

	// PROJECTS ROUTE
	router.HandleFunc("/api/projects", getProjects).Methods("GET")

	// BUYER ORDER HISTORY & MATCH ASSIGNMENTS ROUTES (MOST SPECIFIC - REGISTER FIRST)
	router.HandleFunc("/api/buyer-history/{buyer_id}", getBuyerOrderHistoryHandler).Methods("GET")
	router.HandleFunc("/api/buyer-orders/unmatched", getUnmatchedBuyerOrdersHandler).Methods("GET")
	router.HandleFunc("/api/match-assignments/{buyer_id}", getMatchAssignmentsHandler).Methods("GET")

	// TRADING ROUTES (LESS SPECIFIC - REGISTER AFTER SPECIFIC ROUTES)
	router.HandleFunc("/api/orders", createOrder).Methods("POST")
	router.HandleFunc("/api/orders/all", getAllOrders).Methods("GET")
	router.HandleFunc("/api/orders/{role}/{transaction_type}", getOrders).Methods("GET")
	router.HandleFunc("/api/orders/{role}/{id}", cancelOrder).Methods("DELETE") // NEW ROUTE
	
	router.HandleFunc("/api/top-orders/{role}/{transaction_type}", getTopOrders).Methods("GET")
	router.HandleFunc("/api/top-orders/all", getAllTopOrders).Methods("GET")
	
	router.HandleFunc("/api/matched-orders", getMatchedOrders).Methods("GET")
	router.HandleFunc("/api/matched-orders/user/{user_id}", getUserMatchedOrders).Methods("GET")
	router.HandleFunc("/api/match", triggerMatching).Methods("POST")

	// ADMIN ANALYTICS ROUTES
	router.HandleFunc("/api/admin/analytics", getOverallAnalytics).Methods("GET")
	router.HandleFunc("/api/admin/analytics/project/{project_id}", getProjectAnalytics).Methods("GET")

	// ADMIN DATA MANAGEMENT ROUTES
	router.HandleFunc("/api/admin/clear-database", clearAllData).Methods("POST")
	router.HandleFunc("/api/admin/matching-engine/toggle", toggleMatchingEngine).Methods("POST")
	router.HandleFunc("/api/admin/matching-engine/status", getMatchingStatus).Methods("GET")

	// CIRCUIT BREAKER ROUTES
	router.HandleFunc("/api/admin/circuit-breaker/status", getCircuitBreakerStatuses).Methods("GET")
	router.HandleFunc("/api/admin/circuit-breaker/set", setCircuitBreakerThreshold).Methods("POST")
	router.HandleFunc("/api/admin/circuit-breaker/reset/{project_id}", resetCircuitBreaker).Methods("POST")

	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:3001"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	})

	handler := c.Handler(router)

	port := getEnv("PORT", "8080")
	log.Printf("🚀 Server starting on port %s...", port)
	log.Println("✅ Authentication enabled")
	log.Println("✅ Projects feature enabled (20 projects available)")
	log.Println("✅ Match Type & Market Lead Program enabled")
	log.Println("🛡️ Circuit Breaker system active")
	log.Println("📊 Trading platform ready")
	log.Println("📋 Buyer Order History tracking enabled")
	log.Println("🎯 Match Assignments tracking enabled (Seller quantity breakdown)")
	log.Fatal(http.ListenAndServe(":"+port, handler))
}
