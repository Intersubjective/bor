package plaguewatcher

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/hashicorp/golang-lru/v2/expirable"
	_ "github.com/lib/pq"
)

const BATCH_SIZE_DEFAULT int = 5000
const ONE_MINUTE int64 = 60000

type TransactionsBatch struct {
	transactions []*TxSummaryTransaction
	batch_created_at int64
}
type PlagueWatcher struct {
	db    *sql.DB
	cache *expirable.LRU[string, string]
	peers map[string]*PeerInfo
	batch *TransactionsBatch
	mu    sync.Mutex
}


type PeerInfo struct {
	ID int
}

type PreparedTransaction struct {
	tx_hash       string
	tx_fee        string
	gas_fee_cap   string
	gas_tip_cap   string
	tx_first_seen int64
	receiver      string
	signer        string
	nonce         string
	status        int
	peer_id       string
}

type TxSummaryTransaction struct {
	tx_hash       string
	tx_first_seen int64
}

func Init() (*PlagueWatcher, error) {
	mock_plague := os.Getenv("MOCK_PLAGUE")
	if mock_plague == "true" {
		return &PlagueWatcher{db: nil, cache: nil}, nil
	}

	db, err := OpenDB()
	if err != nil {
		return nil, err
	}
	bf := make(map[string]*PeerInfo)
	cache := expirable.NewLRU[string, string](10000000, nil, time.Hour*144)
	batch := &TransactionsBatch{transactions: make([]*TxSummaryTransaction, 0), batch_created_at: time.Now().UnixMilli()}
	return &PlagueWatcher{db: db, cache: cache, batch: batch, peers: bf}, nil
}

func (pw *PlagueWatcher) handlePeer(peerID string) (int, error) {
	var peer_id_integer int
	peerInfo, exists := pw.peers[peerID]
	if !exists {
		log.Warn("Peer not found", "peerID", peerID)
		err := pw.db.QueryRow(`WITH inserted AS (
			INSERT INTO peer (peer_id) 
			VALUES ($1)
			ON CONFLICT (peer_id) DO UPDATE 
			SET peer_id = EXCLUDED.peer_id
			RETURNING id
		)
		SELECT id FROM inserted
		UNION
		SELECT id FROM peer WHERE peer_id = $1;
		`, peerID).Scan(&peer_id_integer)
		if err != nil {
			log.Warn("Failed to insert peer:", "err", err)
			return 0, err
		}
		pw.peers[peerID] = &PeerInfo{ID: peer_id_integer}
		return peer_id_integer, nil

	}
	return peerInfo.ID, nil
}

func (pw *PlagueWatcher) HandleTxs(txs []*types.Transaction, peerID string) error {
	mock_plague := os.Getenv("MOCK_PLAGUE")
	if mock_plague == "true" {
		return nil
	}
	peerIDint, err := pw.handlePeer(peerID)
	if err != nil {
		return err
	}
	preparedTxs, txs_summary := pw.prepareTransactions(txs, peerID)
	if len(preparedTxs) == 0 && len(txs_summary) == 0 {
		log.Warn("No new txs")
		return nil
	}

	pw.StoreTxPending(preparedTxs, peerID)

	pw.mu.Lock()
	pw.batch.transactions = append(pw.batch.transactions, txs_summary...)
	pw.mu.Unlock()

	if len(pw.batch.transactions) > batchSize() || time.Now().UnixMilli() - pw.batch.batch_created_at > ONE_MINUTE && len(pw.batch.transactions) > 0 {
		log.Info("Inserting batch")
		pw.StoreTxSummary(pw.batch.transactions, peerIDint)
	}
	return nil
}

func (pw *PlagueWatcher) StoreTxPending(txs []*PreparedTransaction, peerID string) {
	sqlstring := `WITH input_rows(tx_hash, tx_fee, gas_fee_cap, gas_tip_cap, tx_first_seen, receiver, signer, nonce, status, peer_id) AS (
		VALUES %s)
		INSERT INTO tx_pending (tx_hash, tx_fee, gas_fee_cap, gas_tip_cap, tx_first_seen, receiver, signer, nonce, status, peer_id)
		SELECT input_rows.tx_hash, input_rows.tx_fee, input_rows.gas_fee_cap, input_rows.gas_tip_cap, input_rows.tx_first_seen, input_rows.receiver, input_rows.signer, input_rows.nonce, input_rows.status, input_rows.peer_id
		FROM input_rows`
	valuesSQL := ""
	for _, tx := range txs {
		valuesSQL += fmt.Sprintf("('%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', %d, '%s'),", tx.tx_hash, tx.tx_fee, tx.gas_fee_cap, tx.gas_tip_cap, tx.tx_first_seen, tx.receiver, tx.signer, tx.nonce, tx.status, tx.peer_id)
	}
	valuesSQL = strings.TrimSuffix(valuesSQL, ",")
	query := fmt.Sprintf(sqlstring, valuesSQL)
	_, err := pw.db.Exec(query)
	if err != nil {
		log.Warn("Failed to insert tx into pool:", "err", err)
	}
}
func (pw *PlagueWatcher) StoreTxSummary(txs []*TxSummaryTransaction, peerID int) {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	sqlstring := `WITH input_rows(tx_hash, peer, tx_first_seen, time) AS (
		VALUES %s
	)
	INSERT INTO tx_summary (tx_hash, peer, tx_first_seen, time)
	SELECT input_rows.tx_hash, input_rows.peer, input_rows.tx_first_seen, input_rows.time
	FROM input_rows
	ON CONFLICT (tx_hash, peer, tx_first_seen) DO NOTHING;`
	valuesSQL := ""
	for _, tx := range txs {
		valuesSQL += fmt.Sprintf("('%s', %d, %d, %d),", tx.tx_hash, peerID, tx.tx_first_seen, tx.tx_first_seen)
	}
	valuesSQL = strings.TrimSuffix(valuesSQL, ",")
	query := fmt.Sprintf(sqlstring, valuesSQL)
	_, err := pw.db.Exec(query)
	if err != nil {
		log.Warn("Failed to insert txs:", "err", err)
	}
	pw.batch.transactions = make([]*TxSummaryTransaction, 0)
	pw.batch.batch_created_at = time.Now().UnixMilli()
}

func (pw *PlagueWatcher) prepareTransactions(txs []*types.Transaction, peerID string) ([]*PreparedTransaction, []*TxSummaryTransaction) {
	//empty slice of prepared transactions
	var preparedTxs []*PreparedTransaction
	var tx_summary []*TxSummaryTransaction
	log.Warn("Preparing txs", "txs", len(txs))
	for _, tx := range txs {
		//check if tx is already in cache
		if _, ok := pw.cache.Get(tx.Hash().Hex()); ok {
			continue
		}
		ts := time.Now().UnixMilli()
		tx_summary = append(tx_summary, &TxSummaryTransaction{
			tx_hash:       tx.Hash().Hex(),
			tx_first_seen: ts,
		})
		pw.cache.Add(tx.Hash().Hex(), tx.Hash().Hex())

		gasFeeCap := tx.GasFeeCap().String()
		gasTipCap := tx.GasTipCap().String()
		fee := strconv.FormatUint(tx.GasPrice().Uint64()*tx.Gas(), 10)
		nonce := strconv.FormatUint(tx.Nonce(), 10)
		signer := types.NewLondonSigner(tx.ChainId())
		addr, err := signer.Sender(tx)
		if err != nil {
			log.Warn("Failed to get the sender:", "err", err)
			addr = common.HexToAddress("0x438308")
		}
		var to string
		if tx.To() == nil {
			to = "0x0"
		} else {
			to = tx.To().Hex()
		}
		preparedTxs = append(preparedTxs, &PreparedTransaction{
			tx_hash:       tx.Hash().Hex(),
			tx_fee:        fee,
			gas_fee_cap:   gasFeeCap,
			gas_tip_cap:   gasTipCap,
			tx_first_seen: ts,
			receiver:      to,
			signer:        addr.Hex(),
			nonce:         nonce,
			status:        1,
			peer_id:       peerID,
		})
	}
	return preparedTxs, tx_summary
}

func (pw *PlagueWatcher) HandleBlocksFetched(block *types.Block, peerID string, peerRemoteAddr string, peerLocalAddr string) error {
	mock_plague := os.Getenv("MOCK_PLAGUE")
	if mock_plague == "true" {
		return nil
	}

	var peer_id_integer int
	err := pw.db.QueryRow(`WITH inserted AS (
		INSERT INTO peer (peer_id) 
		VALUES ($1)
		ON CONFLICT (peer_id) DO UPDATE 
		SET peer_id = EXCLUDED.peer_id
		RETURNING id
	)
	SELECT id FROM inserted
	UNION
	SELECT id FROM peer WHERE peer_id = $1;
	`, peerID).Scan(&peer_id_integer)
	if err != nil {
		log.Warn("Failed to insert peer:", "err", err)
		return err
	}
	ts := time.Now().UnixMilli()
	insertSQL := `INSERT INTO block_fetched(block_hash, block_number, first_seen_ts, peer, peer_remote_addr, peer_local_addr) VALUES($1,$2,$3,$4,$5,$6)`
	log.Warn("Inserting block", "block", block.NumberU64())
	_, err = pw.db.Exec(insertSQL, block.Hash().Hex(), block.NumberU64(), ts, peer_id_integer, peerRemoteAddr, peerLocalAddr)
	if err != nil {
		log.Warn("Failed to insert peer:", "err", err)
		return err
	}
	return err
}

func OpenDB() (*sql.DB, error) {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	log.Warn("Opening DB")

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Warn("Failed to open DB", "err", err)
		return nil, err
	}

	log.Warn("DB opened")
	return db, nil
}

func prepareAndExecQuery(db *sql.DB, queryString string) error {
	query, err := db.Prepare(queryString)
	if err != nil {
		return err
	}
	_, err = query.Exec()
	return err
}
func batchSize() int {
	batch_size := os.Getenv("BATCH_SIZE")
	if batch_size == "" {
		return BATCH_SIZE_DEFAULT
	}
	batch_size_int, err := strconv.Atoi(batch_size)
	if err != nil {
		return BATCH_SIZE_DEFAULT
	}
	return batch_size_int
}	