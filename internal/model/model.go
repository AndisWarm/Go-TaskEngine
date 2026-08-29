// Package model is kept as a compatibility alias for the public model package.
package model

import publicmodel "go-taskengine/model"

type TaskState = publicmodel.TaskState
type TaskMessage = publicmodel.TaskMessage

const (
	StatePending   = publicmodel.StatePending
	StateScheduled = publicmodel.StateScheduled
	StateActive    = publicmodel.StateActive
	StateRetry     = publicmodel.StateRetry
	StateArchived  = publicmodel.StateArchived
	StateCompleted = publicmodel.StateCompleted
)
