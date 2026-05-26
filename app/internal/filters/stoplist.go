package filters

import (
	"errors"
	"strings"
	"sync"

	"top-queries/internal/models"

	goahocorasick "github.com/anknown/ahocorasick"
)

// ErrWordsNotFound is returned when a deletion operation targets terms that are missing from the list.
var ErrWordsNotFound = errors.New("none of the specified words were found in the stop-list")

var runeSlicePool = sync.Pool{
	New: func() interface{} {
		r := make([]rune, 0, 128)
		return &r
	},
}

// StopListFilter provides a thread-safe implementation for blacklisting search queries
// using the Aho-Corasick automaton for multi-pattern matching.
type StopListFilter struct {
	mu        sync.RWMutex
	machine   *goahocorasick.Machine
	stopWords []string
}

// NewStopListFilter initializes a StopListFilter with the given initial blacklisted terms.
func NewStopListFilter(initialWords []string) (*StopListFilter, error) {
	f := &StopListFilter{
		stopWords: make([]string, 0, len(initialWords)),
	}

	for _, w := range initialWords {
		clean := strings.ToLower(strings.TrimSpace(w))
		if clean != "" {
			f.stopWords = append(f.stopWords, clean)
		}
	}

	if err := f.rebuildMachineLocked(); err != nil {
		return nil, err
	}

	return f, nil
}

// Check scans the incoming search event query against the internal automaton.
// It returns true if the query is clean, or false if a forbidden substring is matched.
func (f *StopListFilter) Check(event models.SearchEvent) bool {
	f.mu.RLock()
	m := f.machine
	f.mu.RUnlock()

	if m == nil {
		return true
	}

	lowerQuery := strings.ToLower(event.Query)

	runesPtr := runeSlicePool.Get().(*[]rune)
	runes := (*runesPtr)[:0]

	for _, r := range lowerQuery {
		runes = append(runes, r)
	}

	*runesPtr = runes
	defer runeSlicePool.Put(runesPtr)

	terms := m.MultiPatternSearch(runes, true)

	return len(terms) == 0
}

// Add appends new distinct terms to the filter and performs a thread-safe hot rebuild of the automaton.
func (f *StopListFilter) Add(words []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	inserted := false
	for _, word := range words {
		cleanWord := strings.ToLower(strings.TrimSpace(word))
		if cleanWord == "" {
			continue
		}

		exists := false
		for _, w := range f.stopWords {
			if w == cleanWord {
				exists = true
				break
			}
		}

		if !exists {
			f.stopWords = append(f.stopWords, cleanWord)
			inserted = true
		}
	}

	if inserted {
		return f.rebuildMachineLocked()
	}
	return nil
}

// Delete removes the specified terms from the filter and performs a thread-safe hot rebuild of the automaton.
func (f *StopListFilter) Delete(words []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	toDelete := make(map[string]struct{}, len(words))
	for _, w := range words {
		toDelete[strings.ToLower(strings.TrimSpace(w))] = struct{}{}
	}

	newWords := make([]string, 0, len(f.stopWords))
	removedCount := 0

	for _, w := range f.stopWords {
		if _, shouldDelete := toDelete[w]; shouldDelete {
			removedCount++
			continue
		}
		newWords = append(newWords, w)
	}

	if removedCount == 0 {
		return ErrWordsNotFound
	}

	f.stopWords = newWords
	return f.rebuildMachineLocked()
}

func (f *StopListFilter) rebuildMachineLocked() error {
	if len(f.stopWords) == 0 {
		f.machine = nil
		return nil
	}

	keywords := make([][]rune, len(f.stopWords))
	for i, w := range f.stopWords {
		keywords[i] = []rune(w)
	}

	machine := new(goahocorasick.Machine)
	if err := machine.Build(keywords); err != nil {
		return err
	}

	f.machine = machine
	return nil
}
