package main

import (
	"log"
	"net/http"

	"ride-sharing/shared/contracts"
	"ride-sharing/shared/util"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Driver struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ProfilePicture string `json:"profilePicture"`
	CarPlate       string `json:"carPlate"`
	PackageSlug    string `json:"packageSlug"`
}

func upgradeConnection(w http.ResponseWriter, r *http.Request) (*websocket.Conn, string, bool) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return nil, "", false
	}

	userID := r.URL.Query().Get("userID")
	if userID == "" {
		log.Println("No user ID provided")
		conn.Close()
		return nil, "", false
	}

	return conn, userID, true
}

func readMessages(conn *websocket.Conn) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		log.Printf("Received message: %s", message)
	}
}

func handleRidersWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, _, ok := upgradeConnection(w, r)
	if !ok {
		return
	}
	defer conn.Close()

	readMessages(conn)
}

func handleDriversWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, userID, ok := upgradeConnection(w, r)
	if !ok {
		return
	}
	defer conn.Close()

	packageSlug := r.URL.Query().Get("packageSlug")
	if packageSlug == "" {
		log.Println("No package slug provided")
		return
	}

	msg := contracts.WSMessage{
		Type: "driver.cmd.register",
		Data: Driver{
			ID:             userID,
			Name:           "Priyanshu",
			ProfilePicture: util.GetRandomAvatar(1),
			CarPlate:       "UP1203",
			PackageSlug:    packageSlug,
		},
	}

	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Error sending message: %v", err)
		return
	}

	readMessages(conn)
}