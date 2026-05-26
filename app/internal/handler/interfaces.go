//go:generate mockgen -source=interfaces.go -destination=../mocks/handler.go -package=mocks .
package handler

import "top-queries/internal/models"

// StopWordList defines the behavior for managing hot-swappable stop words.
type StopWordList interface {
	Add(words []string) error
	Delete(words []string) error
}

// RawTopQueriesList provides high-performance access to the pre-rendered JSON top list.
type RawTopQueriesList interface {
	GetTopJSON() []byte
}

// StructedTopQueriesList provides access to the top queries list as a structured data envelope.
type StructedTopQueriesList interface {
	GetTopN(n int) models.TopQueriesResponseEnvelope
}
