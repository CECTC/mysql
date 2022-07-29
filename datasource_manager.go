package mysql

import (
	"context"
	"sync"

	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/hptx/pkg/core"
	"github.com/pkg/errors"
)

var dataSourceManager DataSourceManager

type DataSourceManager struct {
	ResourceCache map[string]*connector
	connections   map[string]*mysqlConn
	sync.Mutex
}

func init() {
	dataSourceManager = DataSourceManager{
		ResourceCache: make(map[string]*connector),
		connections:   make(map[string]*mysqlConn),
	}
}

func GetDataSourceManager() DataSourceManager {
	return dataSourceManager
}

func RegisterResource(dsn string) {
	cfg, err := ParseDSN(dsn)
	if err == nil {
		c := &connector{
			cfg: cfg,
		}
		dataSourceManager.ResourceCache[c.cfg.DBName] = c
		InitTableMetaCache(c.cfg.DBName)
	}
}

func (resourceManager DataSourceManager) GetConnection(resourceID string) *mysqlConn {
	conn, ok := resourceManager.connections[resourceID]
	if ok && conn.IsValid() {
		return conn
	}
	resourceManager.Lock()
	defer resourceManager.Unlock()
	conn, ok = resourceManager.connections[resourceID]
	if ok && conn.IsValid() {
		return conn
	}
	db := resourceManager.ResourceCache[resourceID]
	connection, err := db.Connect(context.Background())
	if err != nil {
		log.Error(err)
	}
	conn = connection.(*mysqlConn)
	resourceManager.connections[resourceID] = conn
	return conn
}

func (resourceManager DataSourceManager) Commit(ctx context.Context, request *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	db := resourceManager.ResourceCache[request.ResourceID]
	if db == nil {
		log.Errorf("Data resource is not exist, resource id: %s", request.ResourceID)
		return request.Status, errors.Errorf("Data resource is not exist, resource id: %s", request.ResourceID)
	}

	undoLogManager := GetUndoLogManager()
	conn := resourceManager.GetConnection(request.ResourceID)

	if conn == nil || !conn.IsValid() {
		return request.Status, errors.New("Connection is not valid")
	}
	err := undoLogManager.DeleteUndoLog(conn, request.XID, request.BranchSessionID)
	if err != nil {
		log.Errorf("[stacktrace]branchCommit failed. xid:[%s], branchID:[%d], resourceID:[%s], branchType:[%d], applicationData:[%v]",
			request.XID, request.BranchID, request.ResourceID, request.Type, request.ApplicationData)
		log.Error(err)
		return request.Status, err
	}
	log.Debugf("branch session committed, branch id: %s, lock key: %s", request.BranchID, request.LockKey)
	return api.Complete, nil
}

func (resourceManager DataSourceManager) Rollback(ctx context.Context, request *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	db := resourceManager.ResourceCache[request.ResourceID]
	if db == nil {
		log.Errorf("Data resource is not exist, resource id: %s", request.ResourceID)
		return request.Status, errors.Errorf("Data resource is not exist, resource id: %s", request.ResourceID)
	}

	undoLogManager := GetUndoLogManager()
	conn, err := db.Connect(context.Background())
	if err != nil {
		log.Error(err)
		return request.Status, err
	}
	defer conn.Close()
	c := conn.(*mysqlConn)
	lockKeys, err := undoLogManager.Undo(c, request.XID, request.BranchSessionID, db.cfg.DBName)
	if err != nil {
		log.Errorf("[stacktrace]branchRollback failed. xid:[%s], branchID:[%d], resourceID:[%s], branchType:[%d], applicationData:[%v]",
			request.XID, request.BranchID, request.ResourceID, request.Type, request.ApplicationData)
		log.Error(err)
		return request.Status, err
	}
	if len(lockKeys) > 0 {
		if _, err := core.GetDistributedTransactionManager().ReleaseLockKeys(context.Background(), request.ResourceID, lockKeys); err != nil {
			log.Errorf("release lock and remove branch session failed, xid = %s, resource_id = %s, lockKeys = %s",
				request.XID, request.ResourceID, lockKeys)
		}
	}
	if len(lockKeys) == 0 {
		if _, err := core.GetDistributedTransactionManager().ReleaseLockKeys(context.Background(),
			request.ResourceID, []string{request.LockKey}); err != nil {
			return request.Status, err
		}
	}
	log.Debugf("branch session rollbacked, branch id: %s, lock key: %s", request.BranchID, request.LockKey)
	return api.Complete, nil
}
