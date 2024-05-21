package event_hub_explorer

import (
	"context"
	"event-hub-explorer/entities"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

type EventModel = entities.EventModel

func event_explorer_main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run main.go <connection_string> <event_hub_name> <consumer_group>")
		os.Exit(1)
	}

	connectionString := os.Args[1]
	eventHubName := os.Args[2]
	consumerGroup := os.Args[3]

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	// WaitGroup to wait for all partition processing to complete
	var wg sync.WaitGroup

	// Create an Event Hub client
	hub, err := azeventhubs.NewConsumerClientFromConnectionString(connectionString, eventHubName, consumerGroup)
	if err != nil {
		fmt.Println("Error creating Event Hub client:", err)
		os.Exit(1)
	}

	// Get partition IDs
	partitionIDs, err := hub.GetPartitionProperties(ctx, 0)
	if err != nil {
		fmt.Println("Error getting partition IDs:", err)
		os.Exit(1)
	}

	// Start a goroutine for each partition to receive events
	for _, partitionID := range partitionIDs {
		wg.Add(1)
		go func(partitionID string) {
			defer wg.Done()
			err := receiveEventsFromPartition(ctx, hub, partitionID)
			if err != nil {
				fmt.Printf("Error receiving events from partition %s: %v\n", partitionID, err)
			}
		}(partitionID)
	}

}

func receiveEventsFromPartition(ctx context.Context, hub *eventhubs.EventHubConsumer, partitionID string) error {
	// Receive events from a partition
	receiver, err := hub.Receive(ctx, partitionID, eventhubs.ReceiveWithStartingPosition(eventhubs.StartOfStream()))
	if err != nil {
		return fmt.Errorf("error creating receiver for partition %s: %v", partitionID, err)
	}

	defer func() {
		_ = receiver.Close(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Partition %s event receiving cancelled.\n", partitionID)
			return nil
		case event := <-receiver.Receive():
			if event.Data == nil {
				continue
			}
			// Assume the event model has the required fields
			eventModel := EventModel{
				PartitionID: partitionID,
				Data:        string(event.Data.Body),
				Timestamp:   event.Data.SystemProperties.EnqueuedTime,
			}
			printEvent(eventModel)
		case <-time.After(30 * time.Second):
			fmt.Printf("No events received from partition %s for 30 seconds.\n", partitionID)
		}
	}
}

func printEvent(event EventModel) {
	// Print event details in tabular format
	fmt.Printf("| %-12s | %-50s | %-25s |\n", event.PartitionID, event.Data, event.Timestamp.Format(time.RFC3339))
}

//go run main.go "your_connection_string" "your_event_hub_name" "$Default"
