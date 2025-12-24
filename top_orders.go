package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

func initTopOrdersTables(database *sql.DB) {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS top_buyer (
			id SERIAL PRIMARY KEY,
			order_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			transaction_id VARCHAR(8) NOT NULL,
			price DECIMAL(10, 2) NOT NULL,
			quantity INTEGER NOT NULL,
			trade_date DATE NOT NULL,
			trade_time TIME NOT NULL,
			transaction_type INTEGER NOT NULL,
			match_type INTEGER NOT NULL DEFAULT 0,
			market_lead_program BOOLEAN NOT NULL DEFAULT false,
			project_id INTEGER DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(order_id)
		)`,
		`CREATE TABLE IF NOT EXISTS top_seller (
			id SERIAL PRIMARY KEY,
			order_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			transaction_id VARCHAR(8) NOT NULL,
			price DECIMAL(10, 2) NOT NULL,
			quantity INTEGER NOT NULL,
			trade_date DATE NOT NULL,
			trade_time TIME NOT NULL,
			transaction_type INTEGER NOT NULL,
			match_type INTEGER NOT NULL DEFAULT 0,
			market_lead_program BOOLEAN NOT NULL DEFAULT false,
			project_id INTEGER DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(order_id)
		)`,
	}

	for _, table := range tables {
		_, err := database.Exec(table)
		if err != nil {
			log.Fatal("Error creating top orders table:", err)
		}
	}

	alterQueries := []string{
		`ALTER TABLE top_buyer ADD COLUMN IF NOT EXISTS match_type INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE top_buyer ADD COLUMN IF NOT EXISTS market_lead_program BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE top_buyer ADD COLUMN IF NOT EXISTS project_id INTEGER DEFAULT 1`,
		`ALTER TABLE top_seller ADD COLUMN IF NOT EXISTS match_type INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE top_seller ADD COLUMN IF NOT EXISTS market_lead_program BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE top_seller ADD COLUMN IF NOT EXISTS project_id INTEGER DEFAULT 1`,
	}

	for _, query := range alterQueries {
		_, err := database.Exec(query)
		if err != nil {
			log.Printf("Warning executing alter query: %v", err)
		}
	}

	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_top_buyer_price ON top_buyer(price DESC)",
		"CREATE INDEX IF NOT EXISTS idx_top_seller_price ON top_seller(price ASC)",
		"CREATE INDEX IF NOT EXISTS idx_buyer_price ON buyer(price DESC)",
		"CREATE INDEX IF NOT EXISTS idx_seller_price ON seller(price ASC)",
		"CREATE INDEX IF NOT EXISTS idx_top_buyer_order_id ON top_buyer(order_id)",
		"CREATE INDEX IF NOT EXISTS idx_top_seller_order_id ON top_seller(order_id)",
		"CREATE INDEX IF NOT EXISTS idx_top_buyer_mlp ON top_buyer(market_lead_program DESC, price DESC)",
		"CREATE INDEX IF NOT EXISTS idx_top_seller_mlp ON top_seller(market_lead_program DESC, price ASC)",
		"CREATE INDEX IF NOT EXISTS idx_top_buyer_qty ON top_buyer(quantity DESC)",
		"CREATE INDEX IF NOT EXISTS idx_top_seller_qty ON top_seller(quantity DESC)",
		"CREATE INDEX IF NOT EXISTS idx_top_buyer_date ON top_buyer(trade_date ASC)",
		"CREATE INDEX IF NOT EXISTS idx_top_seller_date ON top_seller(trade_date ASC)",
		"CREATE INDEX IF NOT EXISTS idx_top_buyer_time ON top_buyer(trade_time ASC)",
		"CREATE INDEX IF NOT EXISTS idx_top_seller_time ON top_seller(trade_time ASC)",
	}

	for _, index := range indexes {
		_, err := database.Exec(index)
		if err != nil {
			log.Printf("Warning creating index: %v", err)
		}
	}

	log.Println("✅ All top orders tables and indexes created with project_id field")
}

func intelligentOrderInsertion(database *sql.DB, order *Order) error {
	tableName := getTableName(order.Role)
	topTableName := getTopTableName(order.Role)

	if tableName == "" || topTableName == "" {
		return fmt.Errorf("invalid role")
	}

	tx, err := database.Begin()
	if err != nil {
		return fmt.Errorf("transaction start failed: %v", err)
	}
	defer tx.Rollback()

	// Step 1: Insert into main table - NOW WITH PROJECT_ID
	query := fmt.Sprintf(`
		INSERT INTO %s (user_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, transaction_id, created_at
	`, tableName)

	var projectID int
	if order.ProjectID != nil {
		projectID = *order.ProjectID
	} else {
		projectID = 1 // Default to project 1 if not provided
	}

	// Fix: order is now a pointer, so updates here reflect in main.go
	err = tx.QueryRow(query, order.UserID, order.Price, order.Quantity,
		order.TradeDate, order.TradeTime, order.TransactionType, order.MatchType, order.MarketLeadProgram, projectID).
		Scan(&order.ID, &order.TransactionID, &order.CreatedAt)

	if err != nil {
		return fmt.Errorf("main table insert failed: %v", err)
	}

	mlpIndicator := ""
	if order.MarketLeadProgram {
		mlpIndicator = " ⭐ MLP"
	}
	log.Printf("🎯 New %s order #%d%s (TXN: %s, price: $%.2f, qty: %d, date: %s, time: %s, user: %d, match_type: %d, project: %d)",
		order.Role, order.ID, mlpIndicator, order.TransactionID, order.Price, order.Quantity,
		order.TradeDate, order.TradeTime, order.UserID, order.MatchType, projectID)

	// Step 2: Check top table count
	var topCount int
	err = tx.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", topTableName)).Scan(&topCount)
	if err != nil {
		return fmt.Errorf("top table count failed: %v", err)
	}

	log.Printf("📊 Current top table status: %d/10 orders", topCount)

	// Step 3: Decide if new order qualifies for top table with TIE-BREAKING
	shouldMoveToTop := false
	var worstOrderID int
	var worstPrice float64

	if topCount < 10 {
		shouldMoveToTop = true
		log.Printf("🔥 Top table has %d/10 orders - new order qualifies for top table", topCount)
	} else {
		switch order.Role {
		case "buyer":
			// MLP BUYERS ALWAYS QUALIFY - BYPASS PRICE CHECK
			if order.MarketLeadProgram {
				shouldMoveToTop = true
				log.Printf("⭐ MLP Buyer detected - PRIORITY ACCESS to top table (bypassing all checks)")

				// Find worst NON-MLP buyer to replace (LOWEST price with tie-breaking)
				err = tx.QueryRow(fmt.Sprintf(`
					SELECT order_id, price FROM %s 
					WHERE market_lead_program = false
					ORDER BY price ASC, quantity ASC, trade_date DESC, trade_time DESC
					LIMIT 1
				`, topTableName)).Scan(&worstOrderID, &worstPrice)

				if err == sql.ErrNoRows {
					// All buyers are MLP, replace the worst MLP buyer by price + tie-breaking
					err = tx.QueryRow(fmt.Sprintf(`
						SELECT order_id, price FROM %s 
						ORDER BY price ASC, quantity ASC, trade_date DESC, trade_time DESC
						LIMIT 1
					`, topTableName)).Scan(&worstOrderID, &worstPrice)

					if err != nil {
						return fmt.Errorf("buyer worst MLP order check failed: %v", err)
					}
					log.Printf("📋 All buyers are MLP - replacing worst MLP buyer ($%.2f)", worstPrice)
				} else if err != nil {
					return fmt.Errorf("buyer worst non-MLP order check failed: %v", err)
				} else {
					log.Printf("🔄 Will replace worst non-MLP buyer #%d ($%.2f)", worstOrderID, worstPrice)
				}
			} else {
				// Normal price-based logic for non-MLP buyers WITH TIE-BREAKING
				err = tx.QueryRow(fmt.Sprintf(`
					SELECT order_id, price FROM %s 
					ORDER BY price ASC, quantity ASC, trade_date DESC, trade_time DESC
					LIMIT 1
				`, topTableName)).Scan(&worstOrderID, &worstPrice)

				if err != nil {
					return fmt.Errorf("buyer worst order check failed: %v", err)
				}

				var worstQty int
				var worstDate string
				var worstTime string
				tx.QueryRow(fmt.Sprintf(`
					SELECT quantity, trade_date, TO_CHAR(trade_time, 'HH24:MI:SS')
					FROM %s WHERE order_id = $1
				`, topTableName), worstOrderID).Scan(&worstQty, &worstDate, &worstTime)

				if order.Price > worstPrice {
					shouldMoveToTop = true
					log.Printf("🔄 New buyer ($%.2f) BEATS worst ($%.2f) on PRICE - will swap",
						order.Price, worstPrice)
				} else if order.Price == worstPrice {
					if order.Quantity > worstQty {
						shouldMoveToTop = true
						log.Printf("🔄 Same price ($%.2f), new qty (%d) BEATS worst (%d) - will swap",
							order.Price, order.Quantity, worstQty)
					} else if order.Quantity == worstQty {
						if order.TradeDate < worstDate {
							shouldMoveToTop = true
							log.Printf("🔄 Same price & qty, new date (%s) BEATS worst (%s) - will swap",
								order.TradeDate, worstDate)
						} else if order.TradeDate == worstDate {
							if order.TradeTime < worstTime {
								shouldMoveToTop = true
								log.Printf("🔄 Same price, qty & date, new time (%s) BEATS worst (%s) - will swap",
									order.TradeTime, worstTime)
							}
						}
					}
				}
			}

		case "seller":
			// MLP SELLERS ALWAYS QUALIFY - BYPASS PRICE CHECK
			if order.MarketLeadProgram {
				shouldMoveToTop = true
				log.Printf("⭐ MLP Seller detected - PRIORITY ACCESS to top table (bypassing all checks)")

				// Find worst NON-MLP seller to replace (HIGHEST price with tie-breaking)
				err = tx.QueryRow(fmt.Sprintf(`
					SELECT order_id, price FROM %s 
					WHERE market_lead_program = false
					ORDER BY price DESC, quantity ASC, trade_date DESC, trade_time DESC
					LIMIT 1
				`, topTableName)).Scan(&worstOrderID, &worstPrice)

				if err == sql.ErrNoRows {
					// All sellers are MLP, replace the worst MLP seller by price + tie-breaking
					err = tx.QueryRow(fmt.Sprintf(`
						SELECT order_id, price FROM %s 
						ORDER BY price DESC, quantity ASC, trade_date DESC, trade_time DESC
						LIMIT 1
					`, topTableName)).Scan(&worstOrderID, &worstPrice)

					if err != nil {
						return fmt.Errorf("seller worst MLP order check failed: %v", err)
					}
					log.Printf("📋 All sellers are MLP - replacing worst MLP seller ($%.2f)", worstPrice)
				} else if err != nil {
					return fmt.Errorf("seller worst non-MLP order check failed: %v", err)
				} else {
					log.Printf("🔄 Will replace worst non-MLP seller #%d ($%.2f)", worstOrderID, worstPrice)
				}
			} else {
				// Normal price-based logic for non-MLP sellers WITH TIE-BREAKING
				err = tx.QueryRow(fmt.Sprintf(`
					SELECT order_id, price FROM %s 
					ORDER BY price DESC, quantity ASC, trade_date DESC, trade_time DESC
					LIMIT 1
				`, topTableName)).Scan(&worstOrderID, &worstPrice)

				if err != nil {
					return fmt.Errorf("seller worst order check failed: %v", err)
				}

				var worstQty int
				var worstDate string
				var worstTime string
				tx.QueryRow(fmt.Sprintf(`
					SELECT quantity, trade_date, TO_CHAR(trade_time, 'HH24:MI:SS')
					FROM %s WHERE order_id = $1
				`, topTableName), worstOrderID).Scan(&worstQty, &worstDate, &worstTime)

				if order.Price < worstPrice {
					shouldMoveToTop = true
					log.Printf("🔄 New seller ($%.2f) BEATS worst ($%.2f) on PRICE - will swap",
						order.Price, worstPrice)
				} else if order.Price == worstPrice {
					if order.Quantity > worstQty {
						shouldMoveToTop = true
						log.Printf("🔄 Same price ($%.2f), new qty (%d) BEATS worst (%d) - will swap",
							order.Price, order.Quantity, worstQty)
					} else if order.Quantity == worstQty {
						if order.TradeDate < worstDate {
							shouldMoveToTop = true
							log.Printf("🔄 Same price & qty, new date (%s) BEATS worst (%s) - will swap",
								order.TradeDate, worstDate)
						} else if order.TradeDate == worstDate {
							if order.TradeTime < worstTime {
								shouldMoveToTop = true
								log.Printf("🔄 Same price, qty & date, new time (%s) BEATS worst (%s) - will swap",
									order.TradeTime, worstTime)
							}
						}
					}
				}
			}
		}
	}

	// Step 4: Handle top table insertion/swapping
	if shouldMoveToTop {
		if worstOrderID > 0 {
			var worstUserID int
			var worstTransactionID string
			var worstQty int
			var worstDate string
			var worstTradeTime time.Time
			var worstTxnType int
			var worstMatchType int
			var worstMLP bool
			var worstProjectID int
			var worstCreatedAt time.Time

			err = tx.QueryRow(fmt.Sprintf(`
				SELECT user_id, transaction_id, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, COALESCE(project_id, 1), created_at
				FROM %s WHERE order_id = $1
			`, topTableName), worstOrderID).Scan(&worstUserID, &worstTransactionID, &worstQty,
				&worstDate, &worstTradeTime, &worstTxnType, &worstMatchType, &worstMLP, &worstProjectID, &worstCreatedAt)

			if err != nil {
				return fmt.Errorf("failed to get worst order data: %v", err)
			}

			var existsInMain bool
			err = tx.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)", tableName),
				worstOrderID).Scan(&existsInMain)
			if err != nil {
				return fmt.Errorf("worst order existence check failed: %v", err)
			}

			if !existsInMain {
				_, err = tx.Exec(fmt.Sprintf(`
					INSERT INTO %s (id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id, created_at)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				`, tableName), worstOrderID, worstUserID, worstTransactionID, worstPrice,
					worstQty, worstDate, worstTradeTime, worstTxnType, worstMatchType, worstMLP, worstProjectID, worstCreatedAt)

				if err != nil {
					return fmt.Errorf("failed to restore worst order to main table: %v", err)
				}
				log.Printf("♻️ Restored order #%d ($%.2f) to main table", worstOrderID, worstPrice)
			}

			_, err = tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE order_id = $1", topTableName), worstOrderID)
			if err != nil {
				return fmt.Errorf("worst order removal from top table failed: %v", err)
			}
			log.Printf("🗑️ Removed order #%d ($%.2f) from top table", worstOrderID, worstPrice)
		}

		var alreadyInTop bool
		err = tx.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE order_id = $1)", topTableName),
			order.ID).Scan(&alreadyInTop)
		if err != nil {
			return fmt.Errorf("new order top table check failed: %v", err)
		}

		if !alreadyInTop {
			_, err = tx.Exec(fmt.Sprintf(`
				INSERT INTO %s (order_id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			`, topTableName), order.ID, order.UserID, order.TransactionID, order.Price,
				order.Quantity, order.TradeDate, order.TradeTime, order.TransactionType, order.MatchType, order.MarketLeadProgram, projectID, order.CreatedAt)

			if err != nil {
				return fmt.Errorf("top table insert failed: %v", err)
			}

			result, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", tableName), order.ID)
			if err != nil {
				return fmt.Errorf("main table deletion failed: %v", err)
			}

			rowsDeleted, _ := result.RowsAffected()
			if rowsDeleted > 0 {
				log.Printf("✅ Order #%d MOVED to top table (deleted from main table)", order.ID)
			}
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %v", err)
	}

	return nil
}

func smartSyncTopOrders(database *sql.DB, role string) error {
	topTable := getTopTableName(role)
	sourceTable := getTableName(role)

	if sourceTable == "" || topTable == "" {
		return fmt.Errorf("invalid role")
	}

	var currentCount int
	err := database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", topTable)).Scan(&currentCount)
	if err != nil {
		return err
	}

	if currentCount >= 10 {
		return nil
	}

	needed := 10 - currentCount

	tx, err := database.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var query string
	if role == "buyer" {
		query = fmt.Sprintf(`
			INSERT INTO %s (order_id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id, created_at)
			SELECT id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, COALESCE(project_id, 1), created_at
			FROM %s
			WHERE id NOT IN (SELECT order_id FROM %s)
			ORDER BY market_lead_program DESC, price DESC, quantity DESC, trade_date ASC, trade_time ASC
			LIMIT $1
		`, topTable, sourceTable, topTable)
	} else {
		query = fmt.Sprintf(`
			INSERT INTO %s (order_id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id, created_at)
			SELECT id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, COALESCE(project_id, 1), created_at
			FROM %s
			WHERE id NOT IN (SELECT order_id FROM %s)
			ORDER BY market_lead_program DESC, price ASC, quantity DESC, trade_date ASC, trade_time ASC
			LIMIT $1
		`, topTable, sourceTable, topTable)
	}

	result, err := tx.Exec(query, needed)
	if err != nil {
		return err
	}

	rowsAdded, _ := result.RowsAffected()

	if rowsAdded > 0 {
		deleteQuery := fmt.Sprintf(`
			DELETE FROM %s 
			WHERE id IN (
				SELECT order_id FROM %s
			)
		`, sourceTable, topTable)

		tx.Exec(deleteQuery)
	}

	return tx.Commit()
}

func syncTopOrders(database *sql.DB, role string) error {
	sourceTable := getTableName(role)
	topTable := getTopTableName(role)

	if sourceTable == "" || topTable == "" {
		return fmt.Errorf("invalid role")
	}

	tx, err := database.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", topTable))
	if err != nil {
		return fmt.Errorf("error clearing top table: %v", err)
	}

	var query string
	if role == "buyer" {
		query = fmt.Sprintf(`
			INSERT INTO %s (order_id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id, created_at)
			SELECT id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, COALESCE(project_id, 1), created_at
			FROM %s
			ORDER BY market_lead_program DESC, price DESC, quantity DESC, trade_date ASC, trade_time ASC
			LIMIT 10
		`, topTable, sourceTable)
	} else {
		query = fmt.Sprintf(`
			INSERT INTO %s (order_id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, project_id, created_at)
			SELECT id, user_id, transaction_id, price, quantity, trade_date, trade_time, transaction_type, match_type, market_lead_program, COALESCE(project_id, 1), created_at
			FROM %s
			ORDER BY market_lead_program DESC, price ASC, quantity DESC, trade_date ASC, trade_time ASC
			LIMIT 10
		`, topTable, sourceTable)
	}

	result, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error inserting top orders: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()

	if rowsAffected > 0 {
		deleteQuery := fmt.Sprintf(`
			DELETE FROM %s 
			WHERE id IN (SELECT order_id FROM %s)
		`, sourceTable, topTable)

		tx.Exec(deleteQuery)
	}

	return tx.Commit()
}

// Replace the existing checkAndTriggerMatching function with this updated version

func checkAndTriggerMatching(database *sql.DB) error {
	// Check if matching is enabled
	matchingEnabledMutex.RLock()
	enabled := matchingEnabled
	matchingEnabledMutex.RUnlock()

	if !enabled {
		log.Println("⏸️  Matching engine is DISABLED - skipping matching")
		return nil
	}

	var buyerCount, sellerCount int

	err := database.QueryRow("SELECT COUNT(*) FROM top_buyer").Scan(&buyerCount)
	if err != nil {
		return fmt.Errorf("error counting top buyers: %v", err)
	}

	err = database.QueryRow("SELECT COUNT(*) FROM top_seller").Scan(&sellerCount)
	if err != nil {
		return fmt.Errorf("error counting top sellers: %v", err)
	}

	log.Printf("📊 Top tables status - Buyers: %d, Sellers: %d", buyerCount, sellerCount)

	// Changed condition: Start matching if BOTH tables have at least 1 order
	if buyerCount >= 1 && sellerCount >= 1 {
		// ========== CHECK CIRCUIT BREAKERS BEFORE MATCHING ==========
		if err := checkAndUpdateCircuitBreakers(database); err != nil {
			log.Printf("⚠️ Warning: Circuit breaker check failed: %v", err)
		}
		// ==========================================================

		log.Println("✅ Both tables have orders - Starting continuous matching...")

		matchStart := time.Now()
		if err := matchAllOrders(database); err != nil {
			return fmt.Errorf("matching failed: %v", err)
		}

		duration := time.Since(matchStart)
		durationMs := float64(duration.Microseconds()) / 1000.0

		log.Printf("⚡ Total matching session completed in %.3fms", durationMs)
	} else {
		if buyerCount == 0 && sellerCount == 0 {
			log.Printf("⏳ Waiting for orders - Need at least 1 buyer and 1 seller")
		} else if buyerCount == 0 {
			log.Printf("⏳ Waiting for buyers - Have %d sellers ready", sellerCount)
		} else if sellerCount == 0 {
			log.Printf("⏳ Waiting for sellers - Have %d buyers ready", buyerCount)
		}
	}

	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func syncAllTopOrders(database *sql.DB) error {
	roles := []string{"buyer", "seller"}

	for _, role := range roles {
		if err := syncTopOrders(database, role); err != nil {
			return err
		}
	}

	return nil
}

func syncTopOrdersIfEmpty(database *sql.DB) error {
	var buyerCount, sellerCount int

	database.QueryRow("SELECT COUNT(*) FROM top_buyer").Scan(&buyerCount)
	database.QueryRow("SELECT COUNT(*) FROM top_seller").Scan(&sellerCount)

	if buyerCount > 0 || sellerCount > 0 {
		return nil
	}

	return syncAllTopOrders(database)
}

func getTopTableName(role string) string {
	switch role {
	case "buyer":
		return "top_buyer"
	case "seller":
		return "top_seller"
	}
	return ""
}

func getTopOrdersData(database *sql.DB, role string, transactionType int) ([]Order, error) {
	topTable := getTopTableName(role)
	if topTable == "" {
		return nil, fmt.Errorf("invalid role")
	}

	var query string
	if role == "buyer" {
		query = fmt.Sprintf(`
			SELECT order_id as id, user_id, transaction_id, price, quantity, trade_date, 
			       TO_CHAR(trade_time, 'HH24:MI:SS') as trade_time, transaction_type, match_type, 
			       market_lead_program, COALESCE(project_id, 1) as project_id, created_at
			FROM %s
			WHERE transaction_type = $1
			ORDER BY market_lead_program DESC, price DESC, quantity DESC, trade_date ASC, trade_time ASC
		`, topTable)
	} else {
		query = fmt.Sprintf(`
			SELECT order_id as id, user_id, transaction_id, price, quantity, trade_date, 
			       TO_CHAR(trade_time, 'HH24:MI:SS') as trade_time, transaction_type, match_type, 
			       market_lead_program, COALESCE(project_id, 1) as project_id, created_at
			FROM %s
			WHERE transaction_type = $1
			ORDER BY market_lead_program DESC, price ASC, quantity DESC, trade_date ASC, trade_time ASC
		`, topTable)
	}

	rows, err := database.Query(query, transactionType)
	if err != nil {
		return nil, fmt.Errorf("error querying top orders: %v", err)
	}
	defer rows.Close()

	orders := []Order{}
	for rows.Next() {
		var order Order
		var projectID int
		err := rows.Scan(&order.ID, &order.UserID, &order.TransactionID, &order.Price, &order.Quantity,
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

	return orders, nil
}