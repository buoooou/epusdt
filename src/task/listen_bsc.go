package task

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type bscRecipientSnapshot struct {
	addrs map[string]struct{}
}

var bscWatchedRecipients atomic.Pointer[bscRecipientSnapshot]

const (
	bscConfirmationPollInterval = 2 * time.Second
	bscConfirmationTimeout      = 2 * time.Minute
	bscConfirmationConcurrency  = 32
	bscSafetyMinConfirmations   = 3
)

var bscConfirmationLimiter = make(chan struct{}, bscConfirmationConcurrency)

type bscChainReader interface {
	TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

// StartBscWebSocketListener drives the BSC listener. Checks chain
// enable status and reloads contract addresses from chain_tokens every
// 10s so admin-side toggles take effect without a restart.
func StartBscWebSocketListener() {
	for {
		if data.IsChainEnabled(mdb.NetworkBsc) {
			if contracts := loadChainTokenContracts(mdb.NetworkBsc, "[BSC-WS]"); len(contracts) > 0 {
				runBscListener(contracts)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func runBscListener(contracts []common.Address) {
	ctx, cancel := chainEnabledWatchdog(mdb.NetworkBsc, "[BSC-WS]", chainTokenFingerprint(mdb.NetworkBsc))
	defer cancel()

	wallets, err := data.GetAvailableWalletAddressByNetwork(mdb.NetworkBsc)
	if err != nil {
		log.Sugar.Errorf("[BSC-WS] Failed to get wallet addresses: %v", err)
		return
	}
	storeBscRecipientsFromWallets(wallets)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w, err := data.GetAvailableWalletAddressByNetwork(mdb.NetworkBsc)
				if err != nil {
					log.Sugar.Warnf("[BSC-WS] refresh wallet addresses: %v", err)
					continue
				}
				storeBscRecipientsFromWallets(w)
			}
		}
	}()

	wsURL, ok := resolveChainWsURL(mdb.NetworkBsc, "[BSC-WS]")
	if !ok {
		return
	}
	log.Sugar.Infof("[BSC-WS] connecting to %s watching %d contract(s)", wsURL, len(contracts))

	query := ethereum.FilterQuery{
		Addresses: contracts,
		Topics:    [][]common.Hash{},
	}

	runEvmWsLogListener(ctx, "[BSC-WS]", wsURL, query, func(client *ethclient.Client, vLog types.Log) {
		if vLog.Removed {
			log.Sugar.Warnf("[BSC-WS] ignore removed log tx=%s block=%d", vLog.TxHash.Hex(), vLog.BlockNumber)
			return
		}
		if len(vLog.Topics) < 3 {
			return
		}

		event := vLog.Topics[0].String()
		if event != transferEventHash.String() {
			return
		}

		toAddr := common.HexToAddress(vLog.Topics[2].Hex())

		if !isWatchedBscRecipient(toAddr) {
			return
		}

		dispatchConfirmedBscTransfer(ctx, client, vLog, toAddr)
	})
}

func dispatchConfirmedBscTransfer(parentCtx context.Context, client bscChainReader, vLog types.Log, toAddr common.Address) {
	select {
	case bscConfirmationLimiter <- struct{}{}:
	default:
		log.Sugar.Errorf("[BSC-WS] confirmation queue full, manual review required tx=%s", vLog.TxHash.Hex())
		return
	}

	go func() {
		defer func() { <-bscConfirmationLimiter }()
		ctx, cancel := context.WithTimeout(parentCtx, bscConfirmationTimeout)
		defer cancel()

		chain, err := data.GetChainByNetwork(mdb.NetworkBsc)
		if err != nil {
			log.Sugar.Errorf("[BSC-WS] load confirmation policy tx=%s: %v", vLog.TxHash.Hex(), err)
			return
		}
		minConfirmations := bscSafetyMinConfirmations
		if chain != nil && chain.MinConfirmations > 0 {
			minConfirmations = max(chain.MinConfirmations, bscSafetyMinConfirmations)
		}

		blockTsMs, err := waitForConfirmedBscLog(ctx, client, vLog, minConfirmations)
		if err != nil {
			log.Sugar.Errorf("[BSC-WS] confirmation failed, manual review required tx=%s: %v", vLog.TxHash.Hex(), err)
			return
		}

		amount := new(big.Int).SetBytes(vLog.Data)
		service.TryProcessEvmERC20Transfer(mdb.NetworkBsc, vLog.Address, toAddr, amount, vLog.TxHash.Hex(), blockTsMs)
	}()
}

func waitForConfirmedBscLog(ctx context.Context, client bscChainReader, vLog types.Log, minConfirmations int) (int64, error) {
	if vLog.Removed {
		return 0, fmt.Errorf("log was removed by a chain reorganization")
	}
	if minConfirmations < 1 {
		minConfirmations = 1
	}
	requiredHead := vLog.BlockNumber + uint64(minConfirmations-1)
	ticker := time.NewTicker(bscConfirmationPollInterval)
	defer ticker.Stop()

	for {
		latest, err := client.HeaderByNumber(ctx, nil)
		if err == nil && latest != nil && latest.Number != nil && latest.Number.Uint64() >= requiredHead {
			break
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("wait for %d confirmation(s): %w", minConfirmations, ctx.Err())
		case <-ticker.C:
		}
	}

	receipt, err := client.TransactionReceipt(ctx, vLog.TxHash)
	if err != nil {
		return 0, fmt.Errorf("fetch confirmed receipt: %w", err)
	}
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful || receipt.BlockNumber == nil {
		return 0, fmt.Errorf("transaction receipt is not successful")
	}
	if receipt.BlockNumber.Uint64() != vLog.BlockNumber {
		return 0, fmt.Errorf("transaction moved from block %d to %d", vLog.BlockNumber, receipt.BlockNumber.Uint64())
	}
	canonicalHeader, err := client.HeaderByNumber(ctx, receipt.BlockNumber)
	if err != nil {
		return 0, fmt.Errorf("fetch canonical block header: %w", err)
	}
	if canonicalHeader == nil || canonicalHeader.Hash() != receipt.BlockHash {
		return 0, fmt.Errorf("transaction block is no longer canonical")
	}
	if vLog.BlockHash != (common.Hash{}) && vLog.BlockHash != receipt.BlockHash {
		return 0, fmt.Errorf("log block hash changed after confirmation")
	}
	if !receiptContainsBscLog(receipt, vLog) {
		return 0, fmt.Errorf("confirmed receipt does not contain the observed transfer log")
	}
	return int64(canonicalHeader.Time) * 1000, nil
}

func receiptContainsBscLog(receipt *types.Receipt, observed types.Log) bool {
	for _, item := range receipt.Logs {
		if item == nil || item.Index != observed.Index || item.TxHash != observed.TxHash || item.Address != observed.Address {
			continue
		}
		if len(item.Topics) != len(observed.Topics) || !bytes.Equal(item.Data, observed.Data) {
			continue
		}
		matched := true
		for i := range item.Topics {
			if item.Topics[i] != observed.Topics[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func storeBscRecipientsFromWallets(wallets []mdb.WalletAddress) int {
	m := make(map[string]struct{})
	for _, w := range wallets {
		a := strings.TrimSpace(w.Address)
		if !common.IsHexAddress(a) {
			continue
		}
		m[strings.ToLower(common.HexToAddress(a).Hex())] = struct{}{}
	}
	bscWatchedRecipients.Store(&bscRecipientSnapshot{addrs: m})
	return len(m)
}

func isWatchedBscRecipient(to common.Address) bool {
	snap := bscWatchedRecipients.Load()
	if snap == nil || len(snap.addrs) == 0 {
		return false
	}
	_, ok := snap.addrs[strings.ToLower(to.Hex())]
	return ok
}
