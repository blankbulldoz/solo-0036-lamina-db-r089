package engine

import (
	"crypto/rand"
	"encoding/hex"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
}
type Transaction struct {
	ID        string `json:"id"`
	Snapshot  uint64 `json:"snapshot"`
	ReadOnly  bool   `json:"read_only"`
	Started   string `json:"started"`
	StagedDDL int    `json:"staged_ddl"`
	writes    map[string]map[string]staged
	relations []catalog.Relation
	indexes   []catalog.Index
}
type Commit struct {
	Sequence        uint64 `json:"sequence"`
	Writes          int    `json:"writes"`
	StagedRelations int    `json:"staged_relations"`
	StagedIndexes   int    `json:"staged_indexes"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}

const maxStagedDDL = 64

func (tx *Transaction) ddlCount() int { return len(tx.relations) + len(tx.indexes) }

// stagedRelation resolves a relation against the transaction's private catalog
// overlay. Staged relations shadow nothing because name reuse is rejected.
func (tx *Transaction) stagedRelation(name string) (catalog.Relation, bool) {
	for _, r := range tx.relations {
		if r.Name == name {
			return r, true
		}
	}
	return catalog.Relation{}, false
}
func (tx *Transaction) stagedIndex(name string) (catalog.Index, bool) {
	for _, i := range tx.indexes {
		if i.Name == name {
			return i, true
		}
	}
	return catalog.Index{}, false
}
func (tx *Transaction) hasRelation(name string) bool {
	if _, ok := tx.stagedRelation(name); ok {
		return true
	}
	return false
}
func (tx *Transaction) hasIndex(name string) bool {
	if _, ok := tx.stagedIndex(name); ok {
		return true
	}
	return false
}

// relationFor resolves a relation the transaction is allowed to act on through
// its own catalog view, returning the staged definition when it created it.
func (tx *Transaction) relationFor(c *catalog.Catalog, name string) (catalog.Relation, error) {
	if r, ok := tx.stagedRelation(name); ok {
		return r, nil
	}
	return c.Relation(name)
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := tx.relationFor(d.catalog, table)
	if err != nil {
		return invalid(err)
	}
	if _, staged := tx.stagedRelation(table); !staged && r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := tx.relationFor(d.catalog, table)
	if err != nil {
		return invalid(err)
	}
	if _, staged := tx.stagedRelation(table); !staged && r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}}
	return nil
}
func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	if _, err := d.tx(id); err != nil {
		return err
	}
	delete(d.transactions, id)
	return nil
}
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	defer delete(d.transactions, id)
	// First-committer-wins for catalog names: a competing immediate DDL or an
	// earlier committing transaction must make this whole transaction lose.
	if err := d.checkCatalogCommit(tx); err != nil {
		d.conflicts++
		return Commit{}, err
	}
	writes := make([]versions.Mutation, 0)
	for table, mutations := range tx.writes {
		for key, m := range mutations {
			if d.store.Latest(table, key) > tx.Snapshot {
				d.conflicts++
				return Commit{}, fault("conflict", "write conflict on %s", table)
			}
			writes = append(writes, m.Mutation)
		}
	}
	// Validate uniqueness against the post-commit catalog and final row state,
	// including unique indexes the same transaction is installing.
	if err := d.validateCommitUniqueness(tx, writes); err != nil {
		d.conflicts++
		return Commit{}, err
	}
	if len(writes) == 0 && tx.ddlCount() == 0 {
		return Commit{Sequence: tx.Snapshot}, nil
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	record := journal.Record{Kind: "commit", Transaction: id, Writes: writes}
	if len(tx.relations) > 0 {
		record.Relations = append(record.Relations, tx.relations...)
	}
	if len(tx.indexes) > 0 {
		record.Indexes = append(record.Indexes, tx.indexes...)
	}
	if err = d.publish(record); err != nil {
		return Commit{}, err
	}
	d.commits++
	return Commit{Sequence: d.sequence, Writes: len(writes), StagedRelations: len(tx.relations), StagedIndexes: len(tx.indexes)}, nil
}
