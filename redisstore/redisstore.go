// Package redisstore exposes the Redis-backed task store.
package redisstore

import (
	internalredisstore "go-taskengine/internal/redisstore"
)

// Store is the Redis-backed implementation of the public storage contract.
type Store = internalredisstore.Store

var (
	ErrNoTask            = internalredisstore.ErrNoTask
	ErrQueueEmpty        = internalredisstore.ErrQueueEmpty
	ErrTaskNotFound      = internalredisstore.ErrTaskNotFound
	ErrTaskExists        = internalredisstore.ErrTaskExists
	ErrInvalidTransition = internalredisstore.ErrInvalidTransition
)

var (
	New            = internalredisstore.New
	TaskKey        = internalredisstore.TaskKey
	PendingKey     = internalredisstore.PendingKey
	PendingRankKey = internalredisstore.PendingRankKey
	ScheduledKey   = internalredisstore.ScheduledKey
	RetryKey       = internalredisstore.RetryKey
	ActiveKey      = internalredisstore.ActiveKey
	LeaseKey       = internalredisstore.LeaseKey
	ArchivedKey    = internalredisstore.ArchivedKey
)
