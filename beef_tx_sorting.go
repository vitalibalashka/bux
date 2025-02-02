package bux

func kahnTopologicalSortTransactions(transactions []*Transaction) []*Transaction {
	txByID, incomingEdgesMap, zeroIncomingEdgeQueue := prepareSortStructures(transactions)
	result := make([]*Transaction, 0, len(transactions))

	for len(zeroIncomingEdgeQueue) > 0 {
		txID := zeroIncomingEdgeQueue[0]
		zeroIncomingEdgeQueue = zeroIncomingEdgeQueue[1:]

		tx := txByID[txID]
		result = append(result, tx)

		zeroIncomingEdgeQueue = removeTxFromIncomingEdges(tx, incomingEdgesMap, zeroIncomingEdgeQueue)
	}

	reverseInPlace(result)
	return result
}

func prepareSortStructures(dag []*Transaction) (txByID map[string]*Transaction, incomingEdgesMap map[string]int, zeroIncomingEdgeQueue []string) {
	dagLen := len(dag)
	txByID = make(map[string]*Transaction, dagLen)
	incomingEdgesMap = make(map[string]int, dagLen)

	for _, tx := range dag {
		txByID[tx.ID] = tx
		incomingEdgesMap[tx.ID] = 0
	}

	calculateIncomingEdges(incomingEdgesMap, txByID)
	zeroIncomingEdgeQueue = getTxWithZeroIncomingEdges(incomingEdgesMap)

	return
}

func calculateIncomingEdges(inDegree map[string]int, txByID map[string]*Transaction) {
	for _, tx := range txByID {
		for _, input := range tx.draftTransaction.Configuration.Inputs {
			inputUtxoTxID := input.UtxoPointer.TransactionID
			if _, ok := txByID[inputUtxoTxID]; ok { // transaction can contains inputs we are not interested in
				inDegree[inputUtxoTxID]++
			}
		}
	}
}

func getTxWithZeroIncomingEdges(incomingEdgesMap map[string]int) []string {
	zeroIncomingEdgeQueue := make([]string, 0, len(incomingEdgesMap))

	for txID, edgeNum := range incomingEdgesMap {
		if edgeNum == 0 {
			zeroIncomingEdgeQueue = append(zeroIncomingEdgeQueue, txID)
		}
	}

	return zeroIncomingEdgeQueue
}

func removeTxFromIncomingEdges(tx *Transaction, incomingEdgesMap map[string]int, zeroIncomingEdgeQueue []string) []string {
	for _, input := range tx.draftTransaction.Configuration.Inputs {
		neighborID := input.UtxoPointer.TransactionID
		incomingEdgesMap[neighborID]--

		if incomingEdgesMap[neighborID] == 0 {
			zeroIncomingEdgeQueue = append(zeroIncomingEdgeQueue, neighborID)
		}
	}

	return zeroIncomingEdgeQueue
}

func reverseInPlace(collection []*Transaction) {
	for i, j := 0, len(collection)-1; i < j; i, j = i+1, j-1 {
		collection[i], collection[j] = collection[j], collection[i]
	}
}
