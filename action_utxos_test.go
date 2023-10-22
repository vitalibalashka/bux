package bux

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)


// Test_UnReserveUtxos will test the method UnReserveUtxos()
func (ts *EmbeddedDBTestSuite) Test_UnReserveUtxos() {
	for _, testCase := range dbTestCases {
		ts.T().Run(testCase.name+" - UnReserveUtxos", func(t *testing.T) {
			tc := ts.genericDBClient(t, testCase.database, false)
			defer tc.Close(tc.ctx)

			ctx := context.Background()

			xPub := newXpub(testXPub, append(tc.client.DefaultModelOptions(), New())...)
			xPub.CurrentBalance = 100000
			err := xPub.Save(ctx)
			require.NoError(t, err)

			destination := newDestination(testXPubID, testLockingScript,
				append(tc.client.DefaultModelOptions(), New())...)
			err = destination.Save(ctx)
			require.NoError(t, err)

			utxo := newUtxo(testXPubID, testTxID, testLockingScript, 0, 100000,
				append(tc.client.DefaultModelOptions(), New())...)
			err = utxo.Save(ctx)
			require.NoError(t, err)

			transaction := newTransaction(testTxHex, append(tc.client.DefaultModelOptions(), New())...)
			err = transaction.Save(ctx)
			require.NoError(t, err)

			draftTransaction := newDraftTransaction(
				testXPub, &TransactionConfig{
					Outputs: []*TransactionOutput{{
						To:       "1A1PjKqjWMNBzTVdcBru27EV1PHcXWc63W",
						Satoshis: 1000,
					}},
					ChangeNumberOfDestinations: 1,
					Sync: &SyncConfig{
						Broadcast:        true,
						BroadcastInstant: false,
						PaymailP2P:       false,
						SyncOnChain:      false,
					},
				},
				append(tc.client.DefaultModelOptions(), New())...,
			)
			err = draftTransaction.Save(ctx)
			require.NoError(t, err)
	
			var utxos []*Utxo
			conditions := &map[string]interface{}{
				draftIDField: draftTransaction.ID,
			}
			utxos, err = tc.client.GetUtxos(ctx, nil, conditions, nil, tc.client.DefaultModelOptions()...)
			for _, utxo := range utxos {
				require.NotZero(t, utxo.ReservedAt)
			}
			tc.client.UnReserveUtxos(ctx, draftTransaction.XpubID, draftTransaction.ID)
			for _, utxo := range utxos {
				require.Zero(t, utxo.ReservedAt)
			}
		})
	}
}