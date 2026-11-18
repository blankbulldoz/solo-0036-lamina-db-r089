package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/versions"
)

// StageRelation adds a relation definition to a transaction's private catalog
// overlay. Nothing is durable or visible to other readers until commit; a
// failed validation or a later rollback leaves no name behind.
func (d *DB) StageRelation(id string, r catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return r, err
	}
	tx, err := d.writable(id)
	if err != nil {
		return r, err
	}
	if err = r.Validate(); err != nil {
		return r, invalid(err)
	}
	if _, exists := d.catalog.Relations[r.Name]; exists || tx.hasRelation(r.Name) {
		return r, fault("conflict", "relation exists")
	}
	if tx.ddlCount() >= maxStagedDDL {
		return r, fault("capacity", "staged catalog change limit %d", maxStagedDDL)
	}
	// Created is assigned at commit from the published sequence. The zero value
	// marks a private, not-yet-durable definition inside the transaction.
	r.Created = 0
	tx.relations = append(tx.relations, r)
	tx.StagedDDL = tx.ddlCount()
	return r, nil
}

// StageIndex adds an index definition to a transaction's private catalog
// overlay. Uniqueness against the eventual committed state is enforced at
// commit; an immediate best-effort check rejects already-visible violations.
func (d *DB) StageIndex(id string, i catalog.Index) (catalog.Index, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return i, err
	}
	tx, err := d.writable(id)
	if err != nil {
		return i, err
	}
	if !catalog.ValidName(i.Name) {
		return i, invalid(fault("invalid", "invalid index name"))
	}
	if _, exists := d.catalog.Indexes[i.Name]; exists || tx.hasIndex(i.Name) {
		return i, fault("conflict", "index already exists")
	}
	if err := d.checkStagedIndexRelation(tx, i); err != nil {
		return i, err
	}
	if tx.ddlCount() >= maxStagedDDL {
		return i, fault("capacity", "staged catalog change limit %d", maxStagedDDL)
	}
	if i.Unique {
		if err := checkUnique(d.committedRows(tx, i.Relation), i); err != nil {
			return i, err
		}
	}
	i.Created = 0
	tx.indexes = append(tx.indexes, i)
	tx.StagedDDL = tx.ddlCount()
	return i, nil
}

func (d *DB) writable(id string) (*Transaction, error) {
	tx, err := d.tx(id)
	if err != nil {
		return nil, err
	}
	if tx.ReadOnly {
		return nil, fault("conflict", "read-only transaction")
	}
	return tx, nil
}

func (d *DB) checkStagedIndexRelation(tx *Transaction, i catalog.Index) error {
	r, err := tx.relationFor(d.catalog, i.Relation)
	if err != nil {
		return invalid(err)
	}
	if _, ok := r.Column(i.Column); !ok {
		return invalid(fault("invalid", "unknown index column"))
	}
	return nil
}

// committedRows returns the row set a transaction currently sees for a table,
// including its staged writes. For a relation the same transaction is creating
// the published chain is empty and only its private rows are visible.
func (d *DB) committedRows(tx *Transaction, table string) map[string]datum.Row {
	rows := d.store.Scan(table, d.sequence)
	overlay(rows, tx.writes, table)
	return rows
}

// checkCatalogCommit re-validates the staged catalog against the live catalog
// under the commit lock. Any name taken by a competing committed DDL, or an
// internally inconsistent staged definition, aborts the whole transaction.
func (d *DB) checkCatalogCommit(tx *Transaction) error {
	stagedRelationNames := map[string]bool{}
	for _, r := range tx.relations {
		if err := r.Validate(); err != nil {
			return invalid(err)
		}
		if _, exists := d.catalog.Relations[r.Name]; exists {
			return fault("conflict", "relation %s was created concurrently", r.Name)
		}
		if stagedRelationNames[r.Name] {
			return fault("conflict", "duplicate staged relation %s", r.Name)
		}
		stagedRelationNames[r.Name] = true
	}
	stagedIndexNames := map[string]bool{}
	for _, i := range tx.indexes {
		if !catalog.ValidName(i.Name) {
			return invalid(fault("invalid", "invalid index name"))
		}
		if _, exists := d.catalog.Indexes[i.Name]; exists {
			return fault("conflict", "index %s was created concurrently", i.Name)
		}
		if stagedIndexNames[i.Name] {
			return fault("conflict", "duplicate staged index %s", i.Name)
		}
		stagedIndexNames[i.Name] = true
		if _, err := tx.relationFor(d.catalog, i.Relation); err != nil {
			return invalid(err)
		}
		r, _ := tx.relationFor(d.catalog, i.Relation)
		if _, ok := r.Column(i.Column); !ok {
			return invalid(fault("invalid", "unknown index column"))
		}
	}
	return nil
}

// validateCommitUniqueness checks every unique index that exists after the
// commit against the final visible row state (committed rows overlaid with the
// transaction's writes), mirroring the immediate CreateIndex admission rule.
func (d *DB) validateCommitUniqueness(tx *Transaction, writes []versions.Mutation) error {
	affected := map[string]bool{}
	for _, m := range writes {
		affected[m.Relation] = true
	}
	for _, i := range tx.indexes {
		if i.Unique {
			affected[i.Relation] = true
		}
	}
	if len(affected) == 0 {
		return nil
	}
	finalRows := map[string]map[string]datum.Row{}
	for table := range affected {
		finalRows[table] = d.committedRows(tx, table)
	}
	for _, i := range d.catalog.Indexes {
		if i.Unique && affected[i.Relation] {
			if err := checkUnique(finalRows[i.Relation], i); err != nil {
				return err
			}
		}
	}
	for _, i := range tx.indexes {
		if i.Unique {
			if err := checkUnique(finalRows[i.Relation], i); err != nil {
				return err
			}
		}
	}
	return nil
}
