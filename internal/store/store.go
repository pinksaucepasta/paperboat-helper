package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const CurrentVersion = 1

var (
	ErrIncompatible = errors.New("store version is incompatible")
	ErrCorrupt      = errors.New("store is corrupt")
	ErrConflict     = errors.New("store state conflict")
	ErrReplayGap    = errors.New("replay gap")
)

type Config struct {
	Root        string
	FailureHook func(string) error
}
type Store struct {
	db   *sql.DB
	hook func(string) error
}

type Session struct {
	ID               string
	Name             string
	CWD              string
	CommandPath      string
	CommandArgs      []string
	CommandEnv       []string
	Columns          uint16
	Rows             uint16
	State            string
	Generation       uint64
	ExitCode         *int
	ExitSignal       string
	ExitedAt         *time.Time
	EarliestSequence uint64
	LatestSequence   uint64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
type OutputEvent struct {
	Channel       byte
	StartSequence uint64
	EndSequence   uint64
	Data          []byte
}
type GapError struct{ Requested, Earliest, Latest uint64 }

func (e *GapError) Error() string {
	return fmt.Sprintf("%v: requested %d retained [%d,%d)", ErrReplayGap, e.Requested, e.Earliest, e.Latest)
}
func (e *GapError) Unwrap() error { return ErrReplayGap }

type InputDecision struct {
	SessionID    string
	ClientID     string
	AttachmentID string
	Generation   uint64
	InputID      string
	Hash         []byte
	Status       string
	BytesWritten int
	ErrorCode    string
	CreatedAt    time.Time
}
type OperationResult struct {
	OperationID string
	RequestHash []byte
	State       string
	Result      []byte
	ErrorCode   string
	CompletedAt time.Time
	ExpiresAt   time.Time
}

func Open(ctx context.Context, config Config) (*Store, error) {
	if !filepath.IsAbs(config.Root) {
		return nil, ErrCorrupt
	}
	if err := ensureDirectory(config.Root); err != nil {
		return nil, err
	}
	path := filepath.Join(config.Root, "state.db")
	if info, err := os.Lstat(path); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return nil, ErrCorrupt
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: path}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, hook: config.FailureHook}
	if err := store.configure(ctx); err != nil {
		db.Close()
		return nil, classifyDBError(err)
	}
	if err := store.migrate(ctx); err != nil {
		db.Close()
		return nil, classifyDBError(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.check(ctx); err != nil {
		db.Close()
		return nil, classifyDBError(err)
	}
	return store, nil
}

func (s *Store) Close() error {
	_, checkpointErr := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return errors.Join(checkpointErr, s.db.Close())
}
func (s *Store) CreateSession(ctx context.Context, session Session) error {
	now := time.Now().UTC()
	if session.CreatedAt.IsZero() {
		session.CreatedAt = now
	}
	if session.UpdatedAt.IsZero() {
		session.UpdatedAt = now
	}
	args, err := json.Marshal(session.CommandArgs)
	if err != nil {
		return err
	}
	environment, err := json.Marshal(session.CommandEnv)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO sessions(id,name,cwd,command_path,command_args,command_env,columns,rows,state,generation,exit_code,exit_signal,exited_at,earliest_sequence,latest_sequence,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, session.ID, session.Name, session.CWD, session.CommandPath, args, environment, session.Columns, session.Rows, session.State, session.Generation, session.ExitCode, nullableString(session.ExitSignal), nullableTime(session.ExitedAt), session.EarliestSequence, session.LatestSequence, session.CreatedAt.UnixNano(), session.UpdatedAt.UnixNano())
	return classify(err)
}

func (s *Store) Sessions(ctx context.Context) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,cwd,command_path,command_args,command_env,columns,rows,state,generation,exit_code,COALESCE(exit_signal,''),exited_at,earliest_sequence,latest_sequence,created_at,updated_at FROM sessions ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []Session
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

func (s *Store) UpdateSession(ctx context.Context, id, expectedState string, next Session) error {
	result, err := s.db.ExecContext(ctx, `UPDATE sessions SET cwd=?,columns=?,rows=?,state=?,generation=?,exit_code=?,exit_signal=?,exited_at=?,earliest_sequence=?,latest_sequence=?,updated_at=? WHERE id=? AND state=?`, next.CWD, next.Columns, next.Rows, next.State, next.Generation, next.ExitCode, nullableString(next.ExitSignal), nullableTime(next.ExitedAt), next.EarliestSequence, next.LatestSequence, time.Now().UTC().UnixNano(), id, expectedState)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) DeleteSession(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE id=? AND state IN ('exited','closed')`, id)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) AppendOutput(ctx context.Context, sessionID string, channel byte, start uint64, data []byte, maxRetained uint64) (OutputEvent, uint64, error) {
	if len(data) == 0 || maxRetained == 0 {
		return OutputEvent{}, 0, ErrConflict
	}
	end := start + uint64(len(data))
	if end < start {
		return OutputEvent{}, 0, ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OutputEvent{}, 0, err
	}
	defer tx.Rollback()
	var latest uint64
	if err := tx.QueryRowContext(ctx, `SELECT latest_sequence FROM sessions WHERE id=?`, sessionID).Scan(&latest); err != nil {
		return OutputEvent{}, 0, err
	}
	if latest != start {
		return OutputEvent{}, 0, ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO output_events(session_id,start_sequence,end_sequence,channel,data) VALUES(?,?,?,?,?)`, sessionID, start, end, channel, append([]byte(nil), data...)); err != nil {
		return OutputEvent{}, 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET latest_sequence=?,updated_at=? WHERE id=?`, end, time.Now().UTC().UnixNano(), sessionID); err != nil {
		return OutputEvent{}, 0, err
	}
	var retained uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(length(data)),0) FROM output_events WHERE session_id=?`, sessionID).Scan(&retained); err != nil {
		return OutputEvent{}, 0, err
	}
	earliest := uint64(0)
	if err := tx.QueryRowContext(ctx, `SELECT earliest_sequence FROM sessions WHERE id=?`, sessionID).Scan(&earliest); err != nil {
		return OutputEvent{}, 0, err
	}
	for retained > maxRetained {
		var eventStart, eventEnd, size uint64
		if err := tx.QueryRowContext(ctx, `SELECT start_sequence,end_sequence,length(data) FROM output_events WHERE session_id=? ORDER BY start_sequence LIMIT 1`, sessionID).Scan(&eventStart, &eventEnd, &size); err != nil {
			return OutputEvent{}, 0, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM output_events WHERE session_id=? AND start_sequence=?`, sessionID, eventStart); err != nil {
			return OutputEvent{}, 0, err
		}
		retained -= size
		earliest = eventEnd
	}
	if retained == 0 {
		earliest = end
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET earliest_sequence=? WHERE id=?`, earliest, sessionID); err != nil {
		return OutputEvent{}, 0, err
	}
	if s.hook != nil {
		if err := s.hook("append_before_commit"); err != nil {
			return OutputEvent{}, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return OutputEvent{}, 0, err
	}
	return OutputEvent{Channel: channel, StartSequence: start, EndSequence: end, Data: append([]byte(nil), data...)}, earliest, nil
}

func (s *Store) Replay(ctx context.Context, sessionID string, from, limit uint64) ([]OutputEvent, uint64, uint64, error) {
	var earliest, latest uint64
	if err := s.db.QueryRowContext(ctx, `SELECT earliest_sequence,latest_sequence FROM sessions WHERE id=?`, sessionID).Scan(&earliest, &latest); err != nil {
		return nil, 0, 0, err
	}
	if from < earliest {
		return nil, earliest, latest, &GapError{from, earliest, latest}
	}
	if from > latest {
		return nil, earliest, latest, ErrConflict
	}
	rows, err := s.db.QueryContext(ctx, `SELECT channel,start_sequence,end_sequence,data FROM output_events WHERE session_id=? AND end_sequence>? ORDER BY start_sequence`, sessionID, from)
	if err != nil {
		return nil, earliest, latest, err
	}
	defer rows.Close()
	var events []OutputEvent
	remaining := limit
	for rows.Next() {
		var event OutputEvent
		if err := rows.Scan(&event.Channel, &event.StartSequence, &event.EndSequence, &event.Data); err != nil {
			return nil, earliest, latest, err
		}
		start := max(from, event.StartSequence)
		end := event.EndSequence
		if limit > 0 && end-start > remaining {
			end = start + remaining
		}
		offsetStart := start - event.StartSequence
		offsetEnd := end - event.StartSequence
		if end > start {
			event.StartSequence = start
			event.EndSequence = end
			event.Data = append([]byte(nil), event.Data[offsetStart:offsetEnd]...)
			events = append(events, event)
			if limit > 0 {
				remaining -= end - start
				if remaining == 0 {
					break
				}
			}
		}
	}
	return events, earliest, latest, rows.Err()
}

func (s *Store) PutInputDecision(ctx context.Context, decision InputDecision) (bool, error) {
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO input_decisions(session_id,client_id,attachment_id,generation,input_id,request_hash,status,bytes_written,error_code,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, decision.SessionID, decision.ClientID, decision.AttachmentID, decision.Generation, decision.InputID, decision.Hash, decision.Status, decision.BytesWritten, nullableString(decision.ErrorCode), decision.CreatedAt.UnixNano())
	if err != nil {
		return false, err
	}
	changed, _ := result.RowsAffected()
	if changed == 1 {
		return true, nil
	}
	var hash []byte
	if err := s.db.QueryRowContext(ctx, `SELECT request_hash FROM input_decisions WHERE session_id=? AND client_id=? AND attachment_id=? AND generation=? AND input_id=?`, decision.SessionID, decision.ClientID, decision.AttachmentID, decision.Generation, decision.InputID).Scan(&hash); err != nil {
		return false, err
	}
	if !bytes.Equal(hash, decision.Hash) {
		return false, ErrConflict
	}
	return false, nil
}

func (s *Store) InputDecisions(ctx context.Context, sessionID string) ([]InputDecision, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id,client_id,attachment_id,generation,input_id,request_hash,status,bytes_written,COALESCE(error_code,''),created_at FROM input_decisions WHERE session_id=? ORDER BY generation,created_at`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var decisions []InputDecision
	for rows.Next() {
		var decision InputDecision
		var created int64
		if err := rows.Scan(&decision.SessionID, &decision.ClientID, &decision.AttachmentID, &decision.Generation, &decision.InputID, &decision.Hash, &decision.Status, &decision.BytesWritten, &decision.ErrorCode, &created); err != nil {
			return nil, err
		}
		decision.CreatedAt = time.Unix(0, created).UTC()
		decisions = append(decisions, decision)
	}
	return decisions, rows.Err()
}

func (s *Store) UpdateInputDecision(ctx context.Context, decision InputDecision) error {
	result, err := s.db.ExecContext(ctx, `UPDATE input_decisions SET status=?,bytes_written=?,error_code=? WHERE session_id=? AND client_id=? AND attachment_id=? AND generation=? AND input_id=? AND request_hash=?`, decision.Status, decision.BytesWritten, nullableString(decision.ErrorCode), decision.SessionID, decision.ClientID, decision.AttachmentID, decision.Generation, decision.InputID, decision.Hash)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) PutOperation(ctx context.Context, operation OperationResult) error {
	_, _, err := s.ReserveOperation(ctx, operation.OperationID, operation.RequestHash, operation.ExpiresAt)
	if err != nil {
		return err
	}
	return s.CompleteOperation(ctx, operation)
}

func (s *Store) ReserveOperation(ctx context.Context, operationID string, requestHash []byte, expiresAt time.Time) (OperationResult, bool, error) {
	result, err := s.db.ExecContext(ctx, `INSERT INTO operation_results(operation_id,request_hash,state,result,error_code,completed_at,expires_at) VALUES(?,?,'pending',NULL,NULL,NULL,?) ON CONFLICT(operation_id) DO NOTHING`, operationID, requestHash, expiresAt.UnixNano())
	if err != nil {
		return OperationResult{}, false, err
	}
	changed, _ := result.RowsAffected()
	if changed == 1 {
		return OperationResult{OperationID: operationID, RequestHash: append([]byte(nil), requestHash...), State: "pending", ExpiresAt: expiresAt}, true, nil
	}
	existing, err := s.operation(ctx, operationID)
	if err != nil {
		return OperationResult{}, false, err
	}
	if !bytes.Equal(existing.RequestHash, requestHash) {
		return OperationResult{}, false, ErrConflict
	}
	return existing, false, nil
}

func (s *Store) CompleteOperation(ctx context.Context, operation OperationResult) error {
	result, err := s.db.ExecContext(ctx, `UPDATE operation_results SET state='completed',result=?,error_code=?,completed_at=?,expires_at=? WHERE operation_id=? AND request_hash=? AND state='pending'`, operation.Result, nullableString(operation.ErrorCode), operation.CompletedAt.UnixNano(), operation.ExpiresAt.UnixNano(), operation.OperationID, operation.RequestHash)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 1 {
		return nil
	}
	existing, err := s.operation(ctx, operation.OperationID)
	if err != nil {
		return err
	}
	if !bytes.Equal(existing.RequestHash, operation.RequestHash) {
		return ErrConflict
	}
	if existing.State == "completed" && bytes.Equal(existing.Result, operation.Result) && existing.ErrorCode == operation.ErrorCode {
		return nil
	}
	return ErrConflict
}

func (s *Store) Operations(ctx context.Context, now time.Time, limit int) ([]OperationResult, error) {
	if limit < 1 {
		return nil, ErrConflict
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM operation_results WHERE state='completed' AND expires_at<=?`, now.UnixNano()); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id,request_hash,state,result,COALESCE(error_code,''),completed_at,expires_at FROM operation_results ORDER BY CASE state WHEN 'pending' THEN 0 ELSE 1 END,completed_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var operations []OperationResult
	for rows.Next() {
		operation, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	return operations, rows.Err()
}

func (s *Store) operation(ctx context.Context, operationID string) (OperationResult, error) {
	return scanOperationRow(s.db.QueryRowContext(ctx, `SELECT operation_id,request_hash,state,result,COALESCE(error_code,''),completed_at,expires_at FROM operation_results WHERE operation_id=?`, operationID))
}

func (s *Store) configure(ctx context.Context) error {
	for _, statement := range []string{"PRAGMA foreign_keys=ON", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA busy_timeout=5000"} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > CurrentVersion {
		return ErrIncompatible
	}
	if version == CurrentVersion {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range migrationV1 {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version=1"); err != nil {
		return err
	}
	if s.hook != nil {
		if err := s.hook("migration_before_commit"); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) check(ctx context.Context) error {
	var result string
	if err := s.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("%w: %s", ErrCorrupt, result)
	}
	return nil
}

var migrationV1 = []string{
	`CREATE TABLE sessions(id TEXT PRIMARY KEY,name TEXT NOT NULL UNIQUE,cwd TEXT NOT NULL,command_path TEXT NOT NULL,command_args BLOB NOT NULL,command_env BLOB NOT NULL,columns INTEGER NOT NULL CHECK(columns BETWEEN 1 AND 1000),rows INTEGER NOT NULL CHECK(rows BETWEEN 1 AND 1000),state TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>=0),exit_code INTEGER,exit_signal TEXT,exited_at INTEGER,earliest_sequence INTEGER NOT NULL CHECK(earliest_sequence>=0),latest_sequence INTEGER NOT NULL CHECK(latest_sequence>=earliest_sequence),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL) STRICT`,
	`CREATE TABLE output_events(session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,start_sequence INTEGER NOT NULL,end_sequence INTEGER NOT NULL,channel INTEGER NOT NULL CHECK(channel IN (1,2)),data BLOB NOT NULL CHECK(length(data)=end_sequence-start_sequence),PRIMARY KEY(session_id,start_sequence)) STRICT`,
	`CREATE TABLE input_decisions(session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,client_id TEXT NOT NULL,attachment_id TEXT NOT NULL,generation INTEGER NOT NULL,input_id TEXT NOT NULL,request_hash BLOB NOT NULL,status TEXT NOT NULL,bytes_written INTEGER NOT NULL,error_code TEXT,created_at INTEGER NOT NULL,PRIMARY KEY(session_id,client_id,attachment_id,generation,input_id)) STRICT`,
	`CREATE TABLE operation_results(operation_id TEXT PRIMARY KEY,request_hash BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN ('pending','completed')),result BLOB,error_code TEXT,completed_at INTEGER,expires_at INTEGER NOT NULL) STRICT`,
}

type scanner interface{ Scan(...any) error }

func scanOperation(row scanner) (OperationResult, error) { return scanOperationRow(row) }

func scanOperationRow(row scanner) (OperationResult, error) {
	var operation OperationResult
	var completed sql.NullInt64
	var expires int64
	if err := row.Scan(&operation.OperationID, &operation.RequestHash, &operation.State, &operation.Result, &operation.ErrorCode, &completed, &expires); err != nil {
		return OperationResult{}, err
	}
	if completed.Valid {
		operation.CompletedAt = time.Unix(0, completed.Int64).UTC()
	}
	operation.ExpiresAt = time.Unix(0, expires).UTC()
	return operation, nil
}

func scanSession(row scanner) (Session, error) {
	var session Session
	var exitCode sql.NullInt64
	var exitedAt sql.NullInt64
	var args, environment []byte
	var created, updated int64
	if err := row.Scan(&session.ID, &session.Name, &session.CWD, &session.CommandPath, &args, &environment, &session.Columns, &session.Rows, &session.State, &session.Generation, &exitCode, &session.ExitSignal, &exitedAt, &session.EarliestSequence, &session.LatestSequence, &created, &updated); err != nil {
		return Session{}, err
	}
	if err := json.Unmarshal(args, &session.CommandArgs); err != nil {
		return Session{}, fmt.Errorf("%w: command args", ErrCorrupt)
	}
	if err := json.Unmarshal(environment, &session.CommandEnv); err != nil {
		return Session{}, fmt.Errorf("%w: command environment", ErrCorrupt)
	}
	if exitCode.Valid {
		value := int(exitCode.Int64)
		session.ExitCode = &value
	}
	if exitedAt.Valid {
		value := time.Unix(0, exitedAt.Int64).UTC()
		session.ExitedAt = &value
	}
	session.CreatedAt = time.Unix(0, created).UTC()
	session.UpdatedAt = time.Unix(0, updated).UTC()
	return session, nil
}
func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UnixNano()
}
func classify(err error) error {
	if err == nil {
		return nil
	}
	return err
}

func classifyDBError(err error) error {
	if err == nil {
		return nil
	}
	var sqliteError interface{ Code() int }
	if errors.As(err, &sqliteError) {
		code := sqliteError.Code() & 0xff
		if code == 11 || code == 26 {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}
	return err
}
func ensureDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrCorrupt
	}
	return os.Chmod(path, 0o700)
}
