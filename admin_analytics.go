package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type ProjectAnalytics struct {
	ProjectID       int     `json:"project_id"`
	ProjectName     string  `json:"project_name"`
	DayStartValue   float64 `json:"day_start_value"`
	DayCloseValue   float64 `json:"day_close_value"`
	HighestValue    float64 `json:"highest_value"`
	LowestValue     float64 `json:"lowest_value"`
	MedianValue     float64 `json:"median_value"`
	TotalMatches    int     `json:"total_matches"`
	TotalVolume     int     `json:"total_volume"`
	LastUpdated     string  `json:"last_updated"`
}

type OverallAnalytics struct {
	DayStartValue   float64            `json:"day_start_value"`
	DayCloseValue   float64            `json:"day_close_value"`
	HighestValue    float64            `json:"highest_value"`
	LowestValue     float64            `json:"lowest_value"`
	MedianValue     float64            `json:"median_value"`
	TotalMatches    int                `json:"total_matches"`
	TotalVolume     int                `json:"total_volume"`
	ProjectStats    []ProjectAnalytics `json:"project_stats"`
	LastUpdated     string             `json:"last_updated"`
}

// Helper function to get user ID from token
func getUserIDFromToken(token string, database *sql.DB) (int, error) {
	token = strings.TrimPrefix(token, "Bearer ")
	
	var userID int
	err := database.QueryRow(`
		SELECT user_id FROM sessions WHERE token = $1
	`, token).Scan(&userID)
	
	return userID, err
}

// Check if user is admin
func isAdmin(userID int, database *sql.DB) bool {
	var isAdminUser bool
	err := database.QueryRow("SELECT COALESCE(is_admin, false) FROM users WHERE id = $1", userID).Scan(&isAdminUser)
	if err != nil {
		log.Printf("Error checking admin status: %v", err)
		return false
	}
	return isAdminUser
}

// Get analytics for a specific project
func getProjectAnalytics(w http.ResponseWriter, r *http.Request) {
	// Verify admin access
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

	analytics, err := calculateProjectAnalytics(db, projectID)
	if err != nil {
		log.Println("Error calculating project analytics:", err)
		http.Error(w, "Error fetching analytics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(analytics)
}

// Get overall analytics across all projects
func getOverallAnalytics(w http.ResponseWriter, r *http.Request) {
	// Verify admin access
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

	analytics, err := calculateOverallAnalytics(db)
	if err != nil {
		log.Println("Error calculating overall analytics:", err)
		http.Error(w, "Error fetching analytics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(analytics)
}

func calculateProjectAnalytics(database *sql.DB, projectID int) (*ProjectAnalytics, error) {
	analytics := &ProjectAnalytics{
		ProjectID: projectID,
	}

	// Get project name
	err := database.QueryRow("SELECT name FROM projects WHERE id = $1", projectID).Scan(&analytics.ProjectName)
	if err != nil {
		analytics.ProjectName = "Unknown Project"
	}

	// Day start value (previous day's last matched price for this project)
	err = database.QueryRow(`
		SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE - INTERVAL '1 day'
		ORDER BY created_at DESC
		LIMIT 1
	`, projectID).Scan(&analytics.DayStartValue)
	if err != nil {
		analytics.DayStartValue = 0
	}

	// Day close value (latest matched price for this project today)
	err = database.QueryRow(`
		SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE
		ORDER BY created_at DESC
		LIMIT 1
	`, projectID).Scan(&analytics.DayCloseValue)
	if err != nil {
		analytics.DayCloseValue = 0
	}

	// Highest value of the day
	err = database.QueryRow(`
		SELECT COALESCE(MAX(GREATEST(buyer_price, seller_price)), 0)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE
	`, projectID).Scan(&analytics.HighestValue)
	if err != nil {
		analytics.HighestValue = 0
	}

	// Lowest value of the day
	err = database.QueryRow(`
		SELECT COALESCE(MIN(LEAST(buyer_price, seller_price)), 0)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE
	`, projectID).Scan(&analytics.LowestValue)
	if err != nil {
		analytics.LowestValue = 0
	}

	// Median value (average of all matched prices today)
	err = database.QueryRow(`
		SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE
	`, projectID).Scan(&analytics.MedianValue)
	if err != nil {
		analytics.MedianValue = 0
	}

	// Total matches today
	err = database.QueryRow(`
		SELECT COUNT(*)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE
	`, projectID).Scan(&analytics.TotalMatches)
	if err != nil {
		analytics.TotalMatches = 0
	}

	// Total volume today
	err = database.QueryRow(`
		SELECT COALESCE(SUM(matched_qty), 0)
		FROM matched_orders
		WHERE project_id = $1
		AND DATE(created_at) = CURRENT_DATE
	`, projectID).Scan(&analytics.TotalVolume)
	if err != nil {
		analytics.TotalVolume = 0
	}

	// Last updated
	database.QueryRow("SELECT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')").Scan(&analytics.LastUpdated)

	return analytics, nil
}

func calculateOverallAnalytics(database *sql.DB) (*OverallAnalytics, error) {
	analytics := &OverallAnalytics{}

	// Overall day start value (previous day's last matched price across all projects)
	database.QueryRow(`
		SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 day'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&analytics.DayStartValue)

	// Overall day close value
	database.QueryRow(`
		SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&analytics.DayCloseValue)

	// Overall highest value
	database.QueryRow(`
		SELECT COALESCE(MAX(GREATEST(buyer_price, seller_price)), 0)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE
	`).Scan(&analytics.HighestValue)

	// Overall lowest value
	database.QueryRow(`
		SELECT COALESCE(MIN(LEAST(buyer_price, seller_price)), 0)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE
	`).Scan(&analytics.LowestValue)

	// Overall median value
	database.QueryRow(`
		SELECT COALESCE(AVG((buyer_price + seller_price) / 2), 0)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE
	`).Scan(&analytics.MedianValue)

	// Overall total matches
	database.QueryRow(`
		SELECT COUNT(*)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE
	`).Scan(&analytics.TotalMatches)

	// Overall total volume
	database.QueryRow(`
		SELECT COALESCE(SUM(matched_qty), 0)
		FROM matched_orders
		WHERE DATE(created_at) = CURRENT_DATE
	`).Scan(&analytics.TotalVolume)

	// Get all project IDs
	rows, err := database.Query("SELECT id FROM projects ORDER BY id ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projectIDs := []int{}
	for rows.Next() {
		var id int
		rows.Scan(&id)
		projectIDs = append(projectIDs, id)
	}

	// Calculate analytics for each project
	analytics.ProjectStats = []ProjectAnalytics{}
	for _, projectID := range projectIDs {
		projectAnalytics, err := calculateProjectAnalytics(database, projectID)
		if err != nil {
			log.Printf("Warning: Error calculating analytics for project %d: %v", projectID, err)
			continue
		}
		analytics.ProjectStats = append(analytics.ProjectStats, *projectAnalytics)
	}

	// Last updated
	database.QueryRow("SELECT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')").Scan(&analytics.LastUpdated)

	return analytics, nil
}

func addAdminColumn(database *sql.DB) {
	_, err := database.Exec(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT false`)
	if err != nil {
		log.Printf("Warning: Could not add is_admin column: %v", err)
	} else {
		log.Println("✅ Admin column added to users table")
	}
}