package task

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type fakeBscChainReader struct {
	latest  *types.Header
	header  *types.Header
	receipt *types.Receipt
	err     error
}

func (f *fakeBscChainReader) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	if f.err != nil {
		return nil, f.err
	}
	if number == nil {
		return f.latest, nil
	}
	return f.header, nil
}

func (f *fakeBscChainReader) TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.receipt, nil
}

func TestWaitForConfirmedBscLogAcceptsCanonicalConfirmedTransfer(t *testing.T) {
	header := &types.Header{Number: big.NewInt(100), Time: 123456}
	txHash := common.HexToHash("0x" + strings.Repeat("1", 64))
	observed := types.Log{
		Address:     common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Topics:      []common.Hash{transferEventHash, {}, common.HexToHash("0x2")},
		Data:        []byte{1, 2, 3},
		BlockNumber: 100,
		BlockHash:   header.Hash(),
		TxHash:      txHash,
		Index:       7,
	}
	receiptLog := observed
	reader := &fakeBscChainReader{
		latest: &types.Header{Number: big.NewInt(102)},
		header: header,
		receipt: &types.Receipt{
			Status:      types.ReceiptStatusSuccessful,
			BlockNumber: big.NewInt(100),
			BlockHash:   header.Hash(),
			Logs:        []*types.Log{&receiptLog},
		},
	}

	got, err := waitForConfirmedBscLog(context.Background(), reader, observed, 3)
	if err != nil {
		t.Fatalf("waitForConfirmedBscLog(): %v", err)
	}
	if got != int64(header.Time)*1000 {
		t.Fatalf("block timestamp = %d, want %d", got, int64(header.Time)*1000)
	}
}

func TestWaitForConfirmedBscLogRejectsReorganizedTransfer(t *testing.T) {
	observedHeader := &types.Header{Number: big.NewInt(100), Time: 123456}
	canonicalHeader := &types.Header{Number: big.NewInt(100), Time: 123457, Extra: []byte("replacement")}
	txHash := common.HexToHash("0x" + strings.Repeat("2", 64))
	observed := types.Log{
		Address:     common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Topics:      []common.Hash{transferEventHash, {}, common.HexToHash("0x3")},
		Data:        []byte{4, 5, 6},
		BlockNumber: 100,
		BlockHash:   observedHeader.Hash(),
		TxHash:      txHash,
		Index:       8,
	}
	receiptLog := observed
	reader := &fakeBscChainReader{
		latest: &types.Header{Number: big.NewInt(102)},
		header: canonicalHeader,
		receipt: &types.Receipt{
			Status:      types.ReceiptStatusSuccessful,
			BlockNumber: big.NewInt(100),
			BlockHash:   observedHeader.Hash(),
			Logs:        []*types.Log{&receiptLog},
		},
	}

	_, err := waitForConfirmedBscLog(context.Background(), reader, observed, 3)
	if err == nil || !strings.Contains(err.Error(), "no longer canonical") {
		t.Fatalf("error = %v, want canonical-block rejection", err)
	}
}

func TestWaitForConfirmedBscLogRejectsRemovedLog(t *testing.T) {
	_, err := waitForConfirmedBscLog(context.Background(), &fakeBscChainReader{err: errors.New("must not call RPC")}, types.Log{Removed: true}, 3)
	if err == nil || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("error = %v, want removed-log rejection", err)
	}
}
