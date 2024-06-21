// Run the kvc service inside the docker container in a seprate process
package kvs

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

type Event struct {
	id  uint64
	ptr *int
}

type EventProcessor struct {
	events chan<- Event
}

func extendsSlice(s *[]Event) {
	*s = append(*s, Event{id: ^uint64(0)})
	fmt.Printf("events(appended) %v\n", s)
}

func modifiesSliceButDoesntExtend(s []Event) {
	s = append(s, Event{id: 0b1110111 & 0b1101101})
	s[1] = Event{id: 8}
}

func returnPtrToSliceOfEvent() *[]Event {
	const n = 4
	se := make([]Event, 4)
	se[0] = Event{id: n << 1}
	se[1] = Event{id: n << 2}
	se[2] = Event{id: n << 3}
	se[3] = Event{id: n << 4}
	fmt.Printf("se %p\n", &se)
	return &se
}

func returnSliceOfEvents() []Event {
	const n = 4
	se := make([]Event, 4)
	se[0] = Event{id: n << 1, ptr: new(int)}
	se[1] = Event{id: n << 2}
	se[2] = Event{id: n << 3}
	se[3] = Event{id: n << 4}
	fmt.Printf("se %p\n", &se)
	fmt.Printf("se[0].ptr %p\n", se[0].ptr)
	return se
}

func modifySlice(slice []Event) []Event {
	slice[0] = Event{id: 6666}
	return slice
}

func TestKVSEcho(t *testing.T) {
	eventProcessor := EventProcessor{}
	// eventProcessor.events = make(chan<- Event)
	fmt.Printf("%p\n", eventProcessor.events)
	eventChan := make(chan Event, 16)
	eventProcessor.events = eventChan
	fmt.Printf("%p\n", eventProcessor.events)

	go func() {
		for i := 0; i < 16; i++ {
			eventProcessor.events <- Event{id: uint64(i+1) << i}
		}
		close(eventChan)
	}()

	// if the channel is empty, the receive operation will block, until a value is send by another goroutine
	for event := range eventChan {
		fmt.Printf("id %d, ", event.id)
	}

	eventSlice := make([]Event, 2)
	eventSlice[0] = Event{id: 2}
	eventSlice[1] = Event{id: 4}

	bufio.NewWriter(os.Stdout).Flush()

	fmt.Printf("events %v\n", eventSlice)
	extendsSlice(&eventSlice)
	fmt.Printf("after events %v\n", eventSlice)

	modifiesSliceButDoesntExtend(eventSlice)
	fmt.Printf("last events %v\n", eventSlice)

	slice := returnPtrToSliceOfEvent()
	fmt.Printf("slice %p\n", slice)
	fmt.Printf("slice %v\n", slice)

	slice2 := returnSliceOfEvents()
	fmt.Printf("slice2[0].ptr %p\n", slice2[0].ptr)

	fmt.Println("******************************************************************")
	s := []Event{{id: 7}, {id: 9}}

	fmt.Printf("Before s[0].id %d\n", s[0])
	s1 := modifySlice(s)
	fmt.Printf("Before s[0].id %d\n", s[0])
	fmt.Printf("After s1[0].id %d\n", s1[0])

	// client := NewClient(&Settings{Endpoint: ":8080", RetriesCount: 4})
	// res := client.Echo(context.Background(), "hello")
	// _ = res
	// assert.Equal(t, nil, res.Err())
	// assert.Equal(t, "HELLO", res.Val())
}
