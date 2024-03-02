package purityrouter

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type PurityRouter struct {
	trustedPeers []string
	topPeersAddr string
}

func NewPurityRouter() (*PurityRouter, error) {
	topPeersAddrEnv := os.Getenv("TOP_PEERS_ADDR")
	if topPeersAddrEnv == "" {
		log.Warn("No dashboard address specified")
	}

	pr := &PurityRouter{
		topPeersAddr: topPeersAddrEnv,
		trustedPeers: []string{},
	}

	// Initial call
	pr.fetchTopPeers()
	log.Warn("Trusted peers:", "pr", pr.trustedPeers)
	// Set up periodic calls
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			pr.fetchTopPeers()
		}
	}()

	return pr, nil
}

func (pr *PurityRouter) fetchTopPeers() error {
	resp, err := http.Get(pr.topPeersAddr + "top-peers")
	if err != nil {
		log.Warn("Error fetching top peers:", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Warn("Unexpected status code:", "status", resp.StatusCode)
		return fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	var peers []string
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		log.Warn("Error decoding response:", err)
		return err
	}

	pr.trustedPeers = peers
	return nil
}

func (pr *PurityRouter) GetTrustedPeers() []string {
	return pr.trustedPeers
}

func (pr *PurityRouter) GetTrustedPeersSet() map[string]bool {
	peersSet := make(map[string]bool)
	for _, peer := range pr.trustedPeers {
		peersSet[peer] = true
	}
	return peersSet
}

func Eth_getBalance(from common.Address) *big.Int {
	url := os.Getenv("ALCHEMY_URL")
	payload := strings.NewReader(fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"params\":[\"%s\"],\"method\":\"eth_getBalance\"}", from.String()))
	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	var response struct {
		Jsonrpc string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  string `json:"result"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		fmt.Println("Error unmarshaling:", err)
		return big.NewInt(0) // or handle the error as you see fit
	}
	balance := new(big.Int)
	log.Warn("Balance:", "balance", response.Result)
	balance.SetString(response.Result[2:], 16) // [2:] to skip the "0x" prefix

	return balance
}

func Eth_getNonce(from common.Address) uint64 {
	// eth_getTransactionCount
	url := os.Getenv("ALCHEMY_URL")
	payload := strings.NewReader(fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"params\":[\"%s\",\"latest\"],\"method\":\"eth_getTransactionCount\"}", from.String()))
	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	var response struct {
		Jsonrpc string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  string `json:"result"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		fmt.Println("Error unmarshaling:", err)
		return 0
	}
	balance := new(big.Int)
	balance.SetString(response.Result[2:], 16)

	// Convert big.Int to uint64
	if balance.BitLen() > 64 {
		fmt.Println("Warning: Balance exceeds uint64 bounds.")
		return 0
	}

	return balance.Uint64()
}
