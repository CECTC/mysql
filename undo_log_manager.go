package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/cectc/dbpack/pkg/log"
	"github.com/pkg/errors"
)

const (
	DeleteUndoLogSql         = "DELETE FROM undo_log WHERE xid = ? and branch_id = ?"
	DeleteUndoLogByCreateSql = "DELETE FROM undo_log WHERE log_created <= ? LIMIT ?"
	InsertUndoLogSql         = `INSERT INTO undo_log (branch_id, xid, context, rollback_info, log_status, log_created, 
		log_modified) VALUES (?, ?, ?, ?, ?, now(), now())`
	SelectUndoLogSql = `SELECT branch_id, xid, context, rollback_info, log_status FROM undo_log 
        WHERE xid = ? AND branch_id = ? FOR UPDATE`
)

type State byte

const (
	Normal State = iota
	GlobalFinished
)

func (state State) String() string {
	switch state {
	case Normal:
		return "Normal"
	case GlobalFinished:
		return "GlobalFinished"
	default:
		return fmt.Sprintf("%d", state)
	}
}

type UndoLogManager struct {
}

func GetUndoLogManager() UndoLogManager {
	return UndoLogManager{}
}

func (manager UndoLogManager) FlushUndoLogs(conn *mysqlConn) error {
	defer func() {
		if err := recover(); err != nil {
			errLog.Print(err)
		}
	}()
	ctx := conn.ctx

	branchUndoLog := &branchUndoLog{
		XID:             ctx.xid,
		BranchSessionID: ctx.branchSessionID,
		SqlUndoLogs:     ctx.sqlUndoItemsBuffer,
	}

	parser := GetUndoLogParser()
	undoLogContent := parser.Encode(branchUndoLog)

	return manager.insertUndoLogWithNormal(conn, ctx.xid, ctx.branchSessionID, buildContext(parser.GetName()), undoLogContent)
}

func (manager UndoLogManager) Undo(conn *mysqlConn, xid string, branchSessionID int64, resourceID string) ([]string, error) {
	var lockKeys []string

	tx, err := conn.Begin()
	if err != nil {
		return nil, err
	}

	args := []driver.Value{xid, branchSessionID}
	rows, err := conn.prepareQuery(SelectUndoLogSql, args)
	if err != nil {
		return nil, err
	}

	exists := false

	undoLogs := make([]*branchUndoLog, 0)

	var branchSessionID2 sql.NullInt64
	var xid2, context sql.NullString
	var rollbackInfo sql.RawBytes
	var state sql.NullInt32

	vals := make([]driver.Value, 5)
	dest := []interface{}{&branchSessionID, &xid, &context, &rollbackInfo, &state}

	for {
		err := rows.Next(vals)
		if err != nil {
			break
		}

		for i, sv := range vals {
			err := convertAssignRows(dest[i], sv)
			if err != nil {
				return nil, fmt.Errorf(`sql: Scan error on column index %d, name %q: %v`, i, rows.Columns()[i], err)
			}
		}

		exists = true

		if State(state.Int32) != Normal {
			log.Debugf("xid %s branch %d, ignore %s undo_log", xid2, branchSessionID2, State(state.Int32).String())
			return nil, nil
		}

		//serializer := getSerializer(context)
		parser := GetUndoLogParser()
		branchUndoLog := parser.Decode(rollbackInfo)
		undoLogs = append(undoLogs, branchUndoLog)
	}
	rows.Close()

	for _, branchUndoLog := range undoLogs {
		sqlUndoLogs := branchUndoLog.SqlUndoLogs
		for _, sqlUndoLog := range sqlUndoLogs {
			tableMeta, err := GetTableMetaCache(conn.cfg.DBName).GetTableMeta(conn, sqlUndoLog.TableName)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return nil, errors.WithStack(err)
				}
				return nil, errors.WithStack(err)
			}

			sqlUndoLog.SetTableMeta(tableMeta)
			err = NewMysqlUndoExecutor(*sqlUndoLog).Execute(conn)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return nil, errors.WithStack(err)
				}
				return nil, errors.WithStack(err)
			}
			lockKeys = append(lockKeys, sqlUndoLog.LockKey)
		}
	}

	if exists {
		_, err := conn.execAlways(DeleteUndoLogSql, args)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return nil, errors.WithStack(err)
			}
			return nil, errors.WithStack(err)
		}
		log.Infof("xid %s branch %d, undo_log deleted with %s", xid, branchSessionID,
			GlobalFinished.String())
		if err := tx.Commit(); err != nil {
			return lockKeys, err
		}
	} else {
		manager.insertUndoLogWithGlobalFinished(conn, xid, branchSessionID,
			buildContext(GetUndoLogParser().GetName()), GetUndoLogParser().GetDefaultContent())
		if err := tx.Commit(); err != nil {
			return lockKeys, err
		}
	}
	return lockKeys, err
}

func (manager UndoLogManager) DeleteUndoLog(conn *mysqlConn, xid string, branchSessionID int64) error {
	args := []driver.Value{xid, branchSessionID}
	result, err := conn.execAlways(DeleteUndoLogSql, args)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	log.Infof("%d undo log deleted by xid:%s and branchSessionID:%d", affectCount, xid, branchSessionID)
	return nil
}

func (manager UndoLogManager) BatchDeleteUndoLog(conn *mysqlConn, xids []string, branchSessionIDs []int64) error {
	if xids == nil || branchSessionIDs == nil || len(xids) == 0 || len(branchSessionIDs) == 0 {
		return nil
	}
	xidSize := len(xids)
	branchSessionIDSize := len(branchSessionIDs)
	batchDeleteSql := toBatchDeleteUndoLogSql(xidSize, branchSessionIDSize)
	var args = make([]driver.Value, 0, xidSize+branchSessionIDSize)
	for _, xid := range xids {
		args = append(args, xid)
	}
	for _, branchSessionID := range branchSessionIDs {
		args = append(args, branchSessionID)
	}
	result, err := conn.execAlways(batchDeleteSql, args)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	fmt.Printf("%d undo log deleted by xids:%v and branchSessionIDs:%v", affectCount, xids, branchSessionIDs)
	return nil
}

func (manager UndoLogManager) DeleteUndoLogByLogCreated(conn *mysqlConn, logCreated time.Time, limitRows int) (sql.Result, error) {
	args := []driver.Value{logCreated, limitRows}
	result, err := conn.execAlways(DeleteUndoLogByCreateSql, args)
	return result, err
}

func toBatchDeleteUndoLogSql(xidSize int, branchSessionIDSize int) string {
	var sb strings.Builder
	fmt.Fprint(&sb, "DELETE FROM undo_log WHERE xid in ")
	fmt.Fprint(&sb, appendInParam(xidSize))
	fmt.Fprint(&sb, " AND branch_id in ")
	fmt.Fprint(&sb, appendInParam(branchSessionIDSize))
	return sb.String()
}

func (manager UndoLogManager) insertUndoLogWithNormal(conn *mysqlConn, xid string, branchSessionID int64,
	rollbackCtx string, undoLogContent []byte) error {
	return manager.insertUndoLog(conn, xid, branchSessionID, rollbackCtx, undoLogContent, Normal)
}

func (manager UndoLogManager) insertUndoLogWithGlobalFinished(conn *mysqlConn, xid string, branchSessionID int64,
	rollbackCtx string, undoLogContent []byte) error {
	return manager.insertUndoLog(conn, xid, branchSessionID, rollbackCtx, undoLogContent, GlobalFinished)
}

func (manager UndoLogManager) insertUndoLog(conn *mysqlConn, xid string, branchSessionID int64,
	rollbackCtx string, undoLogContent []byte, state State) error {
	args := []driver.Value{branchSessionID, xid, rollbackCtx, undoLogContent, state}
	_, err := conn.execAlways(InsertUndoLogSql, args)
	return err
}

func buildContext(serializer string) string {
	return fmt.Sprintf("serializer=%s", serializer)
}

func getSerializer(context string) string {
	return context[10:]
}
