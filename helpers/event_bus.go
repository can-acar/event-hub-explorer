package helpers

import (
	_type "event-hub-explorer/type"
	"reflect"
	"sync"
)

type T = _type.T
type TResult = _type.TResult

type EventBus struct {
	handlers sync.Map
}

var (
	instance *EventBus
	once     sync.Once
)

// GetInstance returns the singleton instance of EventBus
func GetInstance() *EventBus {
	once.Do(func() {
		instance = &EventBus{}
	})
	return instance
}

func (e *EventBus) Send(payload T) TResult {
	eventType := reflect.TypeOf(payload)
	handler, ok := e.handlers.Load(eventType)
	if !ok {
		return nil
	}

	return handler.(func(T) TResult)(payload)
}

func (e *EventBus) SendAsync(payload T) TResult {
	eventType := reflect.TypeOf(payload)
	handler, ok := e.handlers.Load(eventType)
	if !ok {
		return nil
	}

	if result, ok := handler.(func(T) TResult); ok {
		return result(payload)
	}

	asyncHandler := handler.(func(T) TResult)
	return asyncHandler(payload)
}

func (e *EventBus) Publish(payload T) {
	eventType := reflect.TypeOf(payload)

	handler, ok := e.handlers.Load(eventType)

	if !ok {
		return
	}
	handler.(func(T))(payload)
	go func() {
		if asyncHandler, ok := handler.(func(T) TResult); ok {
			asyncHandler(payload)
		}
	}()
}

func (e *EventBus) PublishAsync(payload T) {
	eventType := reflect.TypeOf(payload)

	handler, ok := e.handlers.Load(eventType)

	if !ok {
		return
	}
	handler.(func(T))(payload)
	go func() {
		if asyncHandler, ok := handler.(func(T) TResult); ok {
			asyncHandler(payload)
		}
	}()
}

func (e *EventBus) Register(handler func(T) TResult, event T) {
	eventType := reflect.TypeOf(event)
	e.handlers.Store(eventType, handler)
}

func (e *EventBus) RegisterAsync(handler func(T) TResult, event T) {
	eventType := reflect.TypeOf(event)
	e.handlers.Store(eventType, handler)
}

func (e *EventBus) Unregister(event T) {
	eventType := reflect.TypeOf(event)
	e.handlers.Delete(eventType)
}

func (e *EventBus) UnregisterAsync(event T) {
	eventType := reflect.TypeOf(event)
	e.handlers.Delete(eventType)
}

func (e *EventBus) Clear() {
	e.handlers.Range(func(key, value interface{}) bool {
		e.handlers.Delete(key)
		return true
	})
}

func (e *EventBus) ClearAsync() {
	e.handlers.Range(func(key, value interface{}) bool {
		e.handlers.Delete(key)
		return true
	})
}

func typeOf(i interface{}) reflect.Type {
	return reflect.TypeOf(i)
}
