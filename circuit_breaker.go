package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

type CircuitBreakerSettings struct {
	ProjectID            int     `json:"project_id"`
	ProjectName          string  `json:"project_name"`
	ThresholdPercentage  float64 `json:"threshold_percentage"`
	IsHalted             bool    `json:"is_halted"`
	DayOpenPrice         float64 `json:"day_open_price"`
	CurrentPrice         float64 `json:"current_price"`
	PriceDropPercentage  float64 `json:"price_drop_percentage"`
	HaltedAt             string  `json:"halted_at,omitempty"`
	LastChecked          string  `json:"last_checked"`
}

// Initialize circuit breaker table
func initCircuitBreakerTable(database *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS project_circuit_breakers (
		project_id INTEGER PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
		threshold_percentage DECIMAL(5,2) NOT NULL DEFAULT 0,
		is_halted BOOLEAN DEFAULT false,
		halted_at TIMESTAMP,
		day_open_price DECIMAL(10,2) DEFAULT 0,
		current_price DECIMAL(10,2) DEFAULT 0,
		price_drop_percentage DECIMAL(5,2) DEFAULT 0,
		last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	_, err := database.Exec(query)
	if err != nil {
		log.Fatal("Error creating circuit breaker table:", err)
	}

	log.Println("✅ Circuit breaker table created successfully")
}

// Set circuit breaker threshold for a project
func setCircuitBreakerThreshold(w http.ResponseWriter, r *http.Request) {
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

	var settings struct {
		ProjectID           int     `json:"project_id"`
		ThresholdPercentage float64 `json:"threshold_percentage"`
	}

	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate threshold (0-100%)
	if settings.ThresholdPercentage < 0 || settings.ThresholdPercentage > 100 {
		http.Error(w, "Threshold percentage must be between 0 and 100", http.StatusBadRequest)
		return
	}

	// Insert or update circuit breaker settings
	_, err = db.Exec(`
		INSERT INTO project_circuit_breakers (project_id, threshold_percentage)
		VALUES ($1, $2)
		ON CONFLICT (project_id) 
		DO UPDATE SET threshold_percentage = $2, last_checked = CURRENT_TIMESTAMP
	`, settings.ProjectID, settings.ThresholdPercentage)

	if err != nil {
		log.Println("Error setting circuit breaker:", err)
		http.Error(w, "Error setting circuit breaker", http.StatusInternalServerError)
		return
	}

	log.Printf("✅ Circuit breaker threshold set to %.2f%% for project %d by admin (User ID: %d)",
		settings.ThresholdPercentage, settings.ProjectID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Circuit breaker threshold set to %.2f%% for project %d",
			settings.ThresholdPercentage, settings.ProjectID),
	})
}

// Get all circuit breaker statuses
func getCircuitBreakerStatuses(w http.ResponseWriter, r *http.Request) {
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

	rows, err := db.Query(`
		SELECT 
			p.id, 
			p.name, 
			COALESCE(cb.threshold_percentage, 0),
			COALESCE(cb.is_halted, false),
			COALESCE(cb.day_open_price, 0),
			COALESCE(cb.current_price, 0),
			COALESCE(cb.price_drop_percentage, 0),
			COALESCE(TO_CHAR(cb.halted_at, 'YYYY-MM-DD HH24:MI:SS'), ''),
			COALESCE(TO_CHAR(cb.last_checked, 'YYYY-MM-DD HH24:MI:SS'), '')
		FROM projects p
		LEFT JOIN project_circuit_breakers cb ON p.id = cb.project_id
		ORDER BY p.name
	`)
	if err != nil {
		log.Println("Error fetching circuit breaker statuses:", err)
		http.Error(w, "Error fetching statuses", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	statuses := []CircuitBreakerSettings{}
	for rows.Next() {
		var s CircuitBreakerSettings
		err := rows.Scan(&s.ProjectID, &s.ProjectName, &s.ThresholdPercentage,
			&s.IsHalted, &s.DayOpenPrice, &s.CurrentPrice, &s.PriceDropPercentage,
			&s.HaltedAt, &s.LastChecked)
		if err != nil {
			log.Println("Error scanning row:", err)
			continue
		}
		statuses = append(statuses, s)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// Reset circuit breaker for a project (manual resume)
func resetCircuitBreaker(w http.ResponseWriter, r *http.Request) {
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

	vars := mux.Vars(r)
	projectIDStr := vars["project_id"]
	projectID, err := strconv.Atoi(projectIDStr)
	if err != nil {
		http.Error(w, "Invalid project ID", http.StatusBadRequest)
		return
	}

	_, err = db.Exec(`
		UPDATE project_circuit_breakers
		SET is_halted = false, 
		    halted_at = NULL, 
		    day_open_price = 0,
		    current_price = 0,
		    price_drop_percentage = 0,
		    last_checked = CURRENT_TIMESTAMP
		WHERE project_id = $1
	`, projectID)

	if err != nil {
		log.Println("Error resetting circuit breaker:", err)
		http.Error(w, "Error resetting circuit breaker", http.StatusInternalServerError)
		return
	}

	log.Printf("✅ Circuit breaker manually reset for project %d by admin (User ID: %d)", projectID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Circuit breaker reset for project %d - Trading resumed", projectID),
	})
}

// Check if a project is halted
func isProjectHalted(database *sql.DB, projectID int) (bool, error) {
	var isHalted bool
	err := database.QueryRow(`
		SELECT COALESCE(is_halted, false) 
		FROM project_circuit_breakers 
		WHERE project_id = $1
	`, projectID).Scan(&isHalted)

	if err == sql.ErrNoRows {
		return false, nil // No circuit breaker set = not halted
	}

	return isHalted, err
}

// Check and update circuit breakers based on price movements
func checkAndUpdateCircuitBreakers(database *sql.DB) error {
	rows, err := database.Query(`
		SELECT project_id, threshold_percentage, day_open_price, is_halted
		FROM project_circuit_breakers
		WHERE threshold_percentage > 0
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var projectID int
		var threshold, dayOpenPrice float64
		var isHalted bool

		err := rows.Scan(&projectID, &threshold, &dayOpenPrice, &isHalted)
		if err != nil {
			continue
		}

		// Skip if already halted
		if isHalted {
			continue
		}

		// Get current price (latest matched order today)
		var currentPrice float64
		err = database.QueryRow(`
			SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
			FROM matched_orders
			WHERE project_id = $1
			AND DATE(created_at) = CURRENT_DATE
			ORDER BY created_at DESC
			LIMIT 1
		`, projectID).Scan(&currentPrice)

		if err != nil || currentPrice == 0 {
			continue
		}

		// If no day open price set, use first price of the day
		if dayOpenPrice == 0 {
			err = database.QueryRow(`
				SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
				FROM matched_orders
				WHERE project_id = $1
				AND DATE(created_at) = CURRENT_DATE
				ORDER BY created_at ASC
				LIMIT 1
			`, projectID).Scan(&dayOpenPrice)

			if err != nil || dayOpenPrice == 0 {
				continue
			}

			// Update day open price
			database.Exec(`
				UPDATE project_circuit_breakers
				SET day_open_price = $1
				WHERE project_id = $2
			`, dayOpenPrice, projectID)
		}

		// Calculate price drop percentage
		priceDropPct := ((dayOpenPrice - currentPrice) / dayOpenPrice) * 100

		// Update current price and drop percentage
		database.Exec(`
			UPDATE project_circuit_breakers
			SET current_price = $1, 
			    price_drop_percentage = $2,
			    last_checked = CURRENT_TIMESTAMP
			WHERE project_id = $3
		`, currentPrice, priceDropPct, projectID)

		// Check if threshold breached
		if priceDropPct >= threshold {
			_, err := database.Exec(`
				UPDATE project_circuit_breakers
				SET is_halted = true, 
				    halted_at = CURRENT_TIMESTAMP
				WHERE project_id = $1 AND is_halted = false
			`, projectID)

			if err == nil {
				log.Printf("🚨 CIRCUIT BREAKER TRIGGERED - Project %d halted (%.2f%% drop from $%.2f to $%.2f)",
					projectID, priceDropPct, dayOpenPrice, currentPrice)
			}
		}
	}

	return nil
}

// Reset all circuit breakers at start of new day (run daily)
func resetDailyCircuitBreakers(database *sql.DB) error {
	_, err := database.Exec(`
		UPDATE project_circuit_breakers
		SET is_halted = false,
		    halted_at = NULL,
		    day_open_price = 0,
		    current_price = 0,
		    price_drop_percentage = 0,
		    last_checked = CURRENT_TIMESTAMP
		WHERE DATE(last_checked) < CURRENT_DATE
	`)

	if err != nil {
		return err
	}

	log.Println("✅ Daily circuit breaker reset completed - All projects ready for new trading day")
	return nil
}