package entities

type EventModel struct {
	PartitionId string `json:"partitionId"`
	Data        string `json:"data"`
	Timestamp   string `json:"timestamp"`
}
