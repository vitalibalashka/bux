package bux

func kahnTopologicalSortTransactions(transactions []*Transaction) []*Transaction {
	txById, incomingEdgesMap, zeroIncomingEdgeQueue := prepareSortStructures(transactions)
	result := make([]*Transaction, 0, len(transactions))

	for len(zeroIncomingEdgeQueue) > 0 {
		txID := zeroIncomingEdgeQueue[0]
		zeroIncomingEdgeQueue = zeroIncomingEdgeQueue[1:]

		tx := txById[txID]
		result = append(result, tx)

		zeroIncomingEdgeQueue = removeTxFromIncomingEdges(tx, incomingEdgesMap, zeroIncomingEdgeQueue)
	}

	reverseInPlace(result)
	return result
}

func prepareSortStructures(dag []*Transaction) (txById map[string]*Transaction, incomingEdgesMap map[string]int, zeroIncomingEdgeQueue []string) {
	dagLen := len(dag)
	txById = make(map[string]*Transaction, dagLen)
	incomingEdgesMap = make(map[string]int, dagLen)

	for _, tx := range dag {
		txById[tx.ID] = tx
		incomingEdgesMap[tx.ID] = 0
	}

	calculateIncomingEdges(incomingEdgesMap, dag)
	zeroIncomingEdgeQueue = getTxWithZeroIncomingEdges(incomingEdgesMap)

	return
}

func calculateIncomingEdges(inDegree map[string]int, transactions []*Transaction) {
	for _, tx := range transactions {
		for _, input := range tx.draftTransaction.Configuration.Inputs {
			inDegree[input.UtxoPointer.TransactionID]++
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
