package mr

import "fmt"

func generateMapResultFileName(mapID, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
}

func generateReduceResultFileName(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
}
