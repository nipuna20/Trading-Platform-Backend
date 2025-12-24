package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// Global cache for circuit breakers to avoid DB hits during matching loop
var (
	breakerCache      = make(map[int]bool)
	breakerCacheMutex sync.RWMutex
)

// Update the local cache (call this from checkAndUpdateCircuitBreakers)
func updateBreakerCache(projectID int, isHalted bool) {
	breakerCacheMutex.Lock()
	defer breakerCacheMutex.Unlock()
	breakerCache[projectID] = isHalted
}

func isProjectHaltedCached(projectID int) bool {
	breakerCacheMutex.RLock()
	defer breakerCacheMutex.RUnlock()
	return breakerCache[projectID]
}

type MatchedOrder struct {
	ID                  int       `json:"id"`
	SellerPrice         float64   `json:"seller_price"`
	BuyerPrice          float64   `json:"buyer_price"`
	SellerQty           int       `json:"seller_qty"`
	BuyerQty            int       `json:"buyer_qty"`
	MatchedQty          int       `json:"matched_qty"`
	SellerTime          string    `json:"seller_time"`
	BuyerTime           string    `json:"buyer_time"`
	SellerDate          string    `json:"seller_date"`
	BuyerDate           string    `json:"buyer_date"`
	IncomingTime        time.Time `json:"incoming_time"`
	OutgoingTime        time.Time `json:"outgoing_time"`
	TimeTaken           string    `json:"time_taken"`
	Status              string    `json:"status"`
	TransactionType     int       `json:"transaction_type"`
	BuyerUserID         int       `json:"buyer_user_id"`
	SellerUserID        int       `json:"seller_user_id"`
	BuyerTransactionID  string    `json:"buyer_transaction_id"`
	SellerTransactionID string    `json:"seller_transaction_id"`
	ProjectID           int       `json:"project_id"`
	BuyerOrderID        int       `json:"buyer_order_id"`
	SellerOrderID       int       `json:"seller_order_id"`
	IsMultiMatch        bool      `json:"is_multi_match"`
}

type MatchAssignment struct {
	ID                  int       `json:"id"`
	BuyerOrderID        int       `json:"buyer_order_id"`
	SellerOrderID       int       `json:"seller_order_id"`
	SellerUserID        int       `json:"seller_user_id"`
	SellerTransactionID string    `json:"seller_transaction_id"`
	SellerTotalQty      int       `json:"seller_total_qty"`
	AssignedQty         int       `json:"assigned_qty"`
	SellerPrice         float64   `json:"seller_price"`
	MatchedOrderID      int       `json:"matched_order_id"`
	AssignedAt          time.Time `json:"assigned_at"`
}

var (
	getBuyerQuery        string
	getAllSellersQuery   string
	insertMatchedQuery   string
	countBuyerQuery      string
	countSellerQuery     string

	getBuyerStmt        *sql.Stmt
	getAllSellersStmt   *sql.Stmt
	insertMatchedStmt   *sql.Stmt
	countBuyerStmt      *sql.Stmt
	countSellerStmt     *sql.Stmt

	quietMode bool
)

func initMatchedOrdersTable(database *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS matched_orders (
		id SERIAL PRIMARY KEY,
		seller_price DECIMAL(10, 2) NOT NULL,
		buyer_price DECIMAL(10, 2) NOT NULL,
		seller_qty INTEGER NOT NULL,
		buyer_qty INTEGER NOT NULL,
		matched_qty INTEGER NOT NULL,
		seller_time TIME NOT NULL,
		buyer_time TIME NOT NULL,
		seller_date DATE NOT NULL,
		buyer_date DATE NOT NULL,
		incoming_time TIMESTAMP NOT NULL,
		outgoing_time TIMESTAMP NOT NULL,
		time_taken VARCHAR(50) NOT NULL,
		status VARCHAR(20) DEFAULT 'Closed',
		transaction_type INTEGER NOT NULL,
		buyer_order_id INTEGER NOT NULL,
		seller_order_id INTEGER NOT NULL,
		buyer_user_id INTEGER NOT NULL,
		seller_user_id INTEGER NOT NULL,
		buyer_transaction_id VARCHAR(8) NOT NULL,
		seller_transaction_id VARCHAR(8) NOT NULL,
		project_id INTEGER NOT NULL DEFAULT 1,
		is_multi_match BOOLEAN DEFAULT false,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	_, err := database.Exec(query)
	if err != nil {
		log.Fatal("Error creating matched_orders table:", err)
	}

	alterQueries := []string{
		`ALTER TABLE matched_orders ADD COLUMN IF NOT EXISTS matched_qty INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE matched_orders ADD COLUMN IF NOT EXISTS project_id INTEGER NOT NULL DEFAULT 1`,
		`ALTER TABLE matched_orders ADD COLUMN IF NOT EXISTS is_multi_match BOOLEAN DEFAULT false`,
	}

	for _, q := range alterQueries {
		_, err = database.Exec(q)
		if err != nil {
			log.Printf("Warning: Could not alter table: %v", err)
		}
	}

	// Optimize Indexes for Read Speed
	indexQueries := []string{
		`CREATE INDEX IF NOT EXISTS idx_buyer_price ON buyer (price DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_seller_price ON seller (price ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_top_buyer_order ON top_buyer (order_id)`,
		`CREATE INDEX IF NOT EXISTS idx_top_seller_order ON top_seller (order_id)`,
		`CREATE INDEX IF NOT EXISTS idx_matched_orders_created ON matched_orders (created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_top_buyer_project ON top_buyer (project_id)`, // Added for faster project lookup
	}

	for _, idxQuery := range indexQueries {
		_, err = database.Exec(idxQuery)
		if err != nil {
			log.Printf("Warning: Failed to create index: %v", err)
		}
	}
	log.Println("✅ Matched orders table created with multi-match tracking")
}

func initMatchAssignmentsTable(database *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS match_assignments (
		id SERIAL PRIMARY KEY,
		buyer_order_id INTEGER NOT NULL,
		seller_order_id INTEGER NOT NULL,
		seller_user_id INTEGER NOT NULL,
		seller_transaction_id VARCHAR(8) NOT NULL,
		seller_total_qty INTEGER NOT NULL,
		assigned_qty INTEGER NOT NULL,
		seller_price DECIMAL(10, 2) NOT NULL,
		matched_order_id INTEGER REFERENCES matched_orders(id) ON DELETE CASCADE,
		assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	database.Exec(query)
}

func initBuyerOrderHistoryTable(database *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS buyer_order_history (
		id SERIAL PRIMARY KEY,
		buyer_order_id INTEGER NOT NULL UNIQUE,
		buyer_user_id INTEGER NOT NULL,
		buyer_transaction_id VARCHAR(8) NOT NULL,
		original_price DECIMAL(10, 2) NOT NULL,
		original_qty INTEGER NOT NULL,
		buyer_trade_date DATE NOT NULL,
		buyer_trade_time TIME NOT NULL,
		project_id INTEGER NOT NULL DEFAULT 1,
		total_matched_qty INTEGER NOT NULL DEFAULT 0,
		remaining_qty INTEGER NOT NULL,
		match_count INTEGER NOT NULL DEFAULT 0,
		seller_count INTEGER NOT NULL DEFAULT 0,
		status VARCHAR(20) DEFAULT 'Pending',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	database.Exec(query)
	database.Exec(`ALTER TABLE buyer_order_history ADD COLUMN IF NOT EXISTS seller_count INTEGER NOT NULL DEFAULT 0`)
}

// Optimized: Fire and forget
func recordBuyerOrderHistory(database *sql.DB, order Order) error {
	go func() {
		query := `
			INSERT INTO buyer_order_history 
			(buyer_order_id, buyer_user_id, buyer_transaction_id, original_price, original_qty, 
			 buyer_trade_date, buyer_trade_time, project_id, remaining_qty, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'Pending')
			ON CONFLICT (buyer_order_id) DO NOTHING
		`
		projectID := 1
		if order.ProjectID != nil {
			projectID = *order.ProjectID
		}
		database.Exec(query, order.ID, order.UserID, order.TransactionID, 
			order.Price, order.Quantity, order.TradeDate, order.TradeTime, 
			projectID, order.Quantity)
	}()
	return nil
}

// Optimized: Fire and forget
func updateBuyerOrderHistory(database *sql.DB, buyerID int, matchedQty int) error {
	go func() {
		query := `
			UPDATE buyer_order_history
			SET total_matched_qty = total_matched_qty + $1,
			    remaining_qty = remaining_qty - $1,
			    match_count = match_count + 1,
			    seller_count = seller_count + 1,
			    updated_at = CURRENT_TIMESTAMP,
			    status = CASE 
			        WHEN remaining_qty - $1 <= 0 THEN 'Completed'
			        ELSE 'Partially Matched'
			    END
			WHERE buyer_order_id = $2
		`
		database.Exec(query, matchedQty, buyerID)
	}()
	return nil
}

// Optimized: Fire and forget
func recordMatchAssignment(database *sql.DB, buyerOrderID, sellerOrderID, sellerUserID int, 
	sellerTransactionID string, sellerTotalQty, assignedQty int, sellerPrice float64, matchedOrderID int) error {
	
	go func() {
		query := `
			INSERT INTO match_assignments 
			(buyer_order_id, seller_order_id, seller_user_id, seller_transaction_id, 
			 seller_total_qty, assigned_qty, seller_price, matched_order_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`
		database.Exec(query, buyerOrderID, sellerOrderID, sellerUserID, 
			sellerTransactionID, sellerTotalQty, assignedQty, sellerPrice, matchedOrderID)
	}()
	return nil
}

func getBuyerOrderHistory(database *sql.DB, buyerID int) (*BuyerOrderHistory, error) {
	query := `
		SELECT id, buyer_order_id, buyer_user_id, buyer_transaction_id, original_price, original_qty,
		       buyer_trade_date, TO_CHAR(buyer_trade_time, 'HH24:MI:SS'), project_id, 
		       total_matched_qty, remaining_qty, match_count, seller_count, status, created_at, updated_at
		FROM buyer_order_history
		WHERE buyer_order_id = $1
	`
	var history BuyerOrderHistory
	var tradeTime string
	err := database.QueryRow(query, buyerID).Scan(
		&history.ID, &history.BuyerOrderID, &history.BuyerUserID, &history.BuyerTransactionID,
		&history.OriginalPrice, &history.OriginalQty, &history.BuyerTradeDate, &tradeTime,
		&history.ProjectID, &history.TotalMatchedQty, &history.RemainingQty, &history.MatchCount,
		&history.SellerCount, &history.Status, &history.CreatedAt, &history.UpdatedAt)
	if err != nil {
		return nil, err
	}
	history.BuyerTradeTime = tradeTime
	return &history, nil
}

func getMatchAssignments(database *sql.DB, buyerOrderID int) ([]MatchAssignment, error) {
	query := `
		SELECT id, buyer_order_id, seller_order_id, seller_user_id, seller_transaction_id,
		       seller_total_qty, assigned_qty, seller_price, matched_order_id, assigned_at
		FROM match_assignments
		WHERE buyer_order_id = $1
		ORDER BY assigned_at ASC
	`
	rows, err := database.Query(query, buyerOrderID)
	if err != nil {
		return nil, fmt.Errorf("error querying match assignments: %v", err)
	}
	defer rows.Close()
	assignments := []MatchAssignment{}
	for rows.Next() {
		var ma MatchAssignment
		rows.Scan(&ma.ID, &ma.BuyerOrderID, &ma.SellerOrderID, &ma.SellerUserID,
			&ma.SellerTransactionID, &ma.SellerTotalQty, &ma.AssignedQty,
			&ma.SellerPrice, &ma.MatchedOrderID, &ma.AssignedAt)
		assignments = append(assignments, ma)
	}
	return assignments, nil
}

func initPreparedStatements(database *sql.DB) error {
	var err error

	// UPDATED: Increased LIMIT from 1 to 20 to allow checking multiple buyers
	getBuyerQuery = `
		SELECT order_id, user_id, transaction_id, price, quantity, 
		       trade_date, trade_time, transaction_type, created_at, 
			   match_type, COALESCE(project_id, 1)
		FROM top_buyer
		ORDER BY market_lead_program DESC, price DESC, quantity DESC, trade_date ASC, trade_time ASC
		LIMIT 20
	`
	getBuyerStmt, err = database.Prepare(getBuyerQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare get buyer query: %v", err)
	}

	// UPDATED: Increased LIMIT from 10 to 50 to see sellers for 2nd/3rd ranked buyers
	getAllSellersQuery = `
		SELECT order_id, user_id, transaction_id, price, quantity,
		       trade_date, trade_time, transaction_type, created_at, COALESCE(project_id, 1)
		FROM top_seller
		ORDER BY market_lead_program DESC, price ASC, quantity DESC, trade_date ASC, trade_time ASC
		LIMIT 50
	`
	getAllSellersStmt, err = database.Prepare(getAllSellersQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare get all sellers query: %v", err)
	}

	// Optimized Insert that returns ID
	insertMatchedQuery = `
		INSERT INTO matched_orders 
		(seller_price, buyer_price, seller_qty, buyer_qty, matched_qty, seller_time, buyer_time, 
		 seller_date, buyer_date, incoming_time, outgoing_time, time_taken, status, 
		 transaction_type, buyer_order_id, seller_order_id, buyer_user_id, seller_user_id,
		 buyer_transaction_id, seller_transaction_id, project_id, is_multi_match)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
		RETURNING id
	`
	insertMatchedStmt, err = database.Prepare(insertMatchedQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert matched query: %v", err)
	}

	countBuyerQuery = "SELECT COUNT(*) FROM top_buyer"
	countBuyerStmt, err = database.Prepare(countBuyerQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare count buyer query: %v", err)
	}

	countSellerQuery = "SELECT COUNT(*) FROM top_seller"
	countSellerStmt, err = database.Prepare(countSellerQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare count seller query: %v", err)
	}

	log.Println("✅ Prepared statements initialized with optimizations")
	return nil
}

func isTransactionTypeCompatible(buyerType, sellerType int) bool {
	if buyerType == 2 || sellerType == 2 {
		return true
	}
	return buyerType == sellerType
}

func matchAllOrdersContinuous(database *sql.DB) error {
	if err := initPreparedStatements(database); err != nil {
		return err
	}

	matchCount := 0
	totalStartTime := time.Now()
	
	// Update cache once at start of loop
	checkAndUpdateCircuitBreakers(database)

	for {
		var buyerCount, sellerCount int
		// Run counts in parallel? No, overhead of goroutines > query time for simple count
		countBuyerStmt.QueryRow().Scan(&buyerCount)
		countSellerStmt.QueryRow().Scan(&sellerCount)

		if buyerCount < 1 || sellerCount < 1 {
			// Trigger a final sync when idle to ensure tables are full for next run
			go func() {
				syncAllTopOrders(database)
			}()
			break
		}

		matchMade, err := matchOrders(database)
		if err != nil {
			return fmt.Errorf("match failed: %v", err)
		}

		if matchMade {
			matchCount++
			if !quietMode {
				// log.Printf("✅ Match #%d completed", matchCount) 
				// Logging slows down high frequency loops, enable only if debugging
			}
		} else {
			// No match found despite having orders (incompatible types/prices)
			// Break to prevent infinite loop of non-matching orders
			break
		}
	}
	
	if matchCount > 0 {
		duration := time.Since(totalStartTime)
		log.Printf("⚡ Batch complete: %d matches in %.3fms", matchCount, float64(duration.Microseconds())/1000.0)
	}

	return nil
}

func matchOrders(database *sql.DB) (bool, error) {
	matchingStartTime := time.Now()

	type OrderData struct {
		ID              int
		UserID          int
		TransactionID   string
		Price           float64
		Quantity        int
		Date            string
		TradeTime       time.Time
		Time            string
		TransactionType int
		ProjectID       int
		CreatedAt       time.Time
		MatchType       int // Only used for Buyer
	}

	// 1. Get Top 20 Buyers (Loop through them)
	buyerRows, err := getBuyerStmt.Query()
	if err != nil {
		return false, fmt.Errorf("get buyers failed: %v", err)
	}
	defer buyerRows.Close()

	for buyerRows.Next() {
		var buyer OrderData
		err := buyerRows.Scan(
			&buyer.ID, &buyer.UserID, &buyer.TransactionID, &buyer.Price, &buyer.Quantity,
			&buyer.Date, &buyer.TradeTime, &buyer.TransactionType, &buyer.CreatedAt,
			&buyer.MatchType, &buyer.ProjectID,
		)
		if err != nil {
			continue // Skip bad row
		}

		buyer.Time = buyer.TradeTime.Format("15:04:05")

		// Circuit Breaker Check
		if isProjectHaltedCached(buyer.ProjectID) {
			// log.Printf("🛑 Circuit Breaker: Project %d halted - Skipping buyer %d", buyer.ProjectID, buyer.ID)
			continue
		}

		// 2. Get Top 50 Sellers (Fetch specifically for this iteration)
		sellersRows, err := getAllSellersStmt.Query()
		if err != nil {
			log.Printf("Warning: Failed to fetch sellers for buyer %d: %v", buyer.ID, err)
			continue
		}
		
		var compatibleSellers []OrderData
		for sellersRows.Next() {
			var seller OrderData
			err := sellersRows.Scan(
				&seller.ID, &seller.UserID, &seller.TransactionID, &seller.Price, &seller.Quantity,
				&seller.Date, &seller.TradeTime, &seller.TransactionType, &seller.CreatedAt, &seller.ProjectID,
			)
			if err != nil { continue }
			
			seller.Time = seller.TradeTime.Format("15:04:05")

			// STRICT Project ID Match
			if buyer.ProjectID != seller.ProjectID {
				continue
			}

			if !isTransactionTypeCompatible(buyer.TransactionType, seller.TransactionType) {
				continue
			}

			// Exact vs Highest-to-Lowest Logic
			if buyer.MatchType == 0 {
				if buyer.Price == seller.Price { compatibleSellers = append(compatibleSellers, seller) }
			} else {
				if buyer.Price > seller.Price { compatibleSellers = append(compatibleSellers, seller) }
			}
		}
		sellersRows.Close() // Close immediately to free resources

		if len(compatibleSellers) == 0 {
			// This buyer has no matches, try the NEXT buyer in the loop (e.g. Project 5)
			continue
		}

		// 3. Match Found! Execute Transaction
		tx, err := database.Begin()
		if err != nil { return false, err }
		defer tx.Rollback()

		remainingBuyerQty := buyer.Quantity
		matchedSellers := 0
		shouldDeleteBuyer := false
		isMultiMatch := false

		// Prepare data for async history updates
		type MatchRecord struct {
			BuyerID, SellerID, SellerUserID, MatchedQty int
			SellerTxnID string
			SellerPrice float64
			MatchedID int
		}
		var matchRecords []MatchRecord

		for _, seller := range compatibleSellers {
			if remainingBuyerQty <= 0 { break }

			var incomingTime, outgoingTime time.Time
			if buyer.CreatedAt.Before(seller.CreatedAt) {
				incomingTime = buyer.CreatedAt; outgoingTime = seller.CreatedAt
			} else {
				incomingTime = seller.CreatedAt; outgoingTime = buyer.CreatedAt
			}

			var matchedQty int
			var shouldDeleteSeller bool

			if seller.Quantity >= remainingBuyerQty {
				matchedQty = remainingBuyerQty
				shouldDeleteSeller = (seller.Quantity == remainingBuyerQty)
				shouldDeleteBuyer = true
			} else {
				matchedQty = seller.Quantity
				shouldDeleteSeller = true
			}

			if matchedSellers > 0 { isMultiMatch = true }
			timeTaken := fmt.Sprintf("%.3f ms", float64(time.Since(matchingStartTime).Microseconds())/1000.0)

			var matchedTxnType int
			if buyer.TransactionType == 2 && seller.TransactionType != 2 {
				matchedTxnType = seller.TransactionType
			} else if seller.TransactionType == 2 && buyer.TransactionType != 2 {
				matchedTxnType = buyer.TransactionType
			} else {
				matchedTxnType = buyer.TransactionType
			}

			// Insert Match
			insertTxStmt := tx.Stmt(insertMatchedStmt)
			var matchedID int
			err = insertTxStmt.QueryRow(
				seller.Price, buyer.Price, seller.Quantity, buyer.Quantity, matchedQty,
				seller.Time, buyer.Time, seller.Date, buyer.Date,
				incomingTime, outgoingTime, timeTaken, "Closed",
				matchedTxnType, buyer.ID, seller.ID, buyer.UserID, seller.UserID,
				buyer.TransactionID, seller.TransactionID,
				buyer.ProjectID, isMultiMatch,
			).Scan(&matchedID)
			if err != nil { return false, fmt.Errorf("insert matched failed: %v", err) }

			// Store for async processing
			matchRecords = append(matchRecords, MatchRecord{
				BuyerID: buyer.ID, SellerID: seller.ID, SellerUserID: seller.UserID,
				MatchedQty: matchedQty, SellerTxnID: seller.TransactionID, 
				SellerPrice: seller.Price, MatchedID: matchedID,
			})

			// Update Top Seller Table
			if shouldDeleteSeller {
				_, err = tx.Exec("DELETE FROM top_seller WHERE order_id = $1", seller.ID)
			} else {
				remaining := seller.Quantity - matchedQty
				_, err = tx.Exec("UPDATE top_seller SET quantity = $1 WHERE order_id = $2", remaining, seller.ID)
				go func(rid, qty int) {
					database.Exec("UPDATE seller SET quantity = $1 WHERE id = $2", qty, rid)
				}(seller.ID, remaining)
			}
			if err != nil { return false, fmt.Errorf("seller update failed: %v", err) }

			remainingBuyerQty -= matchedQty
			matchedSellers++
		}

		// Update Top Buyer Table
		if shouldDeleteBuyer {
			_, err = tx.Exec("DELETE FROM top_buyer WHERE order_id = $1", buyer.ID)
		} else {
			_, err = tx.Exec("UPDATE top_buyer SET quantity = $1 WHERE order_id = $2", remainingBuyerQty, buyer.ID)
			go func(bid, qty int) {
				database.Exec("UPDATE buyer SET quantity = $1 WHERE id = $2", qty, bid)
			}(buyer.ID, remainingBuyerQty)
		}
		if err != nil { return false, fmt.Errorf("buyer update failed: %v", err) }

		// Commit
		if err = tx.Commit(); err != nil { return false, fmt.Errorf("commit failed: %v", err) }

		// --- ASYNC TASKS ---
		go func() {
			for _, rec := range matchRecords {
				updateBuyerOrderHistory(database, rec.BuyerID, rec.MatchedQty)
				recordMatchAssignment(database, rec.BuyerID, rec.SellerID, rec.SellerUserID, 
					rec.SellerTxnID, rec.MatchedQty+0, rec.MatchedQty, rec.SellerPrice, rec.MatchedID)
			}
			if shouldDeleteBuyer {
				smartSyncTopOrders(database, "buyer")
			}
			smartSyncTopOrders(database, "seller")
		}()

		// IMPORTANT: Return true immediately to restart main loop from top priority
		return true, nil
	}

	// If we loop through ALL top 20 buyers and find NO matches, return false
	return false, nil
}

func matchAllOrders(database *sql.DB) error {
	return matchAllOrdersContinuous(database)
}

func getMatchedOrdersByUser(database *sql.DB, userID int) ([]MatchedOrder, error) {
	query := `
		SELECT id, seller_price, buyer_price, seller_qty, buyer_qty, matched_qty,
		       seller_time, buyer_time, seller_date, buyer_date,
		       incoming_time, outgoing_time, time_taken, status, transaction_type,
		       buyer_user_id, seller_user_id, buyer_transaction_id, seller_transaction_id,
		       COALESCE(project_id, 1) as project_id, buyer_order_id, seller_order_id,
		       COALESCE(is_multi_match, false) as is_multi_match
		FROM matched_orders
		WHERE buyer_user_id = $1 OR seller_user_id = $1
		ORDER BY created_at DESC
	`
	rows, err := database.Query(query, userID)
	if err != nil { return nil, err }
	defer rows.Close()

	matches := []MatchedOrder{}
	for rows.Next() {
		var m MatchedOrder
		rows.Scan(&m.ID, &m.SellerPrice, &m.BuyerPrice, &m.SellerQty, &m.BuyerQty, &m.MatchedQty,
			&m.SellerTime, &m.BuyerTime, &m.SellerDate, &m.BuyerDate,
			&m.IncomingTime, &m.OutgoingTime, &m.TimeTaken, &m.Status, &m.TransactionType,
			&m.BuyerUserID, &m.SellerUserID, &m.BuyerTransactionID, &m.SellerTransactionID,
			&m.ProjectID, &m.BuyerOrderID, &m.SellerOrderID, &m.IsMultiMatch)
		matches = append(matches, m)
	}
	return matches, nil
}

func getMatchedOrdersData(database *sql.DB) ([]MatchedOrder, error) {
	query := `
		SELECT id, seller_price, buyer_price, seller_qty, buyer_qty, matched_qty,
		       seller_time, buyer_time, seller_date, buyer_date,
		       incoming_time, outgoing_time, time_taken, status, transaction_type,
		       buyer_user_id, seller_user_id, buyer_transaction_id, seller_transaction_id,
		       COALESCE(project_id, 1) as project_id, buyer_order_id, seller_order_id,
		       COALESCE(is_multi_match, false) as is_multi_match
		FROM matched_orders
		ORDER BY created_at DESC
	`
	rows, err := database.Query(query)
	if err != nil { return nil, err }
	defer rows.Close()

	matches := []MatchedOrder{}
	for rows.Next() {
		var m MatchedOrder
		rows.Scan(&m.ID, &m.SellerPrice, &m.BuyerPrice, &m.SellerQty, &m.BuyerQty, &m.MatchedQty,
			&m.SellerTime, &m.BuyerTime, &m.SellerDate, &m.BuyerDate,
			&m.IncomingTime, &m.OutgoingTime, &m.TimeTaken, &m.Status, &m.TransactionType,
			&m.BuyerUserID, &m.SellerUserID, &m.BuyerTransactionID, &m.SellerTransactionID,
			&m.ProjectID, &m.BuyerOrderID, &m.SellerOrderID, &m.IsMultiMatch)
		matches = append(matches, m)
	}
	return matches, nil
}