package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/query"
)

func (d *DB) Query(q query.Request) (query.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return query.Result{}, err
	}
	at := d.sequence
	var tx *Transaction
	if q.Transaction != "" {
		if q.At != nil {
			return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
		}
		var err error
		tx, err = d.tx(q.Transaction)
		if err != nil {
			return query.Result{}, err
		}
		at = tx.Snapshot
	} else if q.At != nil {
		at = *q.At
	}
	// A transaction resolves relations through its private catalog overlay, so
	// it can read objects it has staged but nobody else can see yet.
	var r catalogRelation
	if tx != nil {
		resolved, err := tx.relationFor(d.catalog, q.Relation)
		if err != nil {
			return query.Result{}, invalid(err)
		}
		r = catalogRelation{Relation: resolved, staged: tx.hasRelation(q.Relation)}
	} else {
		resolved, err := d.catalog.Relation(q.Relation)
		if err != nil {
			return query.Result{}, invalid(err)
		}
		r = catalogRelation{Relation: resolved}
	}
	if at < d.horizon || at > d.sequence {
		return query.Result{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	if !r.staged && at < r.Created {
		return query.Result{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if err := query.Bind(r.Relation, q); err != nil {
		return query.Result{}, invalid(err)
	}
	if tx != nil && (len(tx.writes[q.Relation]) > 0 || r.staged || tx.ddlCount() > 0) {
		// Private rows and not-yet-published catalog objects have no index
		// candidates; the planner must observe only the committed catalog.
		q.ForceScan = true
	}
	plan, keys := query.Choose(d.catalog, d.indexes, q, at, len(d.store.Tables[q.Relation]))
	rows := map[string]datum.Row{}
	if plan.Access == "index" {
		for _, key := range keys {
			if row, exists := d.store.Read(q.Relation, key, at); exists {
				rows[key] = row
			}
		}
	} else {
		rows = d.store.Scan(q.Relation, at)
	}
	if tx != nil {
		overlay(rows, tx.writes, q.Relation)
	}
	result, err := query.Execute(r.Relation, q, rows, plan, keys)
	d.queries++
	if err != nil {
		return result, invalid(err)
	}
	return result, nil
}

// catalogRelation pairs a resolved relation definition with whether it exists
// only in the staging transaction's private overlay.
type catalogRelation struct {
	catalog.Relation
	staged bool
}

func (d *DB) Get(id, table string, key datum.Value) (datum.Row, error) {
	d.mu.Lock()
	r, err := d.catalog.Relation(table)
	if err == nil && id != "" {
		// Prefer a staged definition when the caller's transaction created it.
		if tx, ok := d.transactions[id]; ok {
			if staged, isStaged := tx.stagedRelation(table); isStaged {
				r = staged
			}
		}
	}
	d.mu.Unlock()
	if err != nil {
		return nil, invalid(err)
	}
	col, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != col.Type {
		return nil, fault("invalid", "invalid primary value")
	}
	q := query.Request{Relation: table, Transaction: id, Limit: 1}
	q.Where = []query.Predicate{{Column: r.Primary, Op: "=", Value: key}}
	result, err := d.Query(q)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fault("not_found", "tuple not found")
	}
	return result.Rows[0], nil
}
