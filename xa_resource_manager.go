package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/hptx/pkg/resource"
	"github.com/pkg/errors"
)

var xaResourceManager ResourceManager

type ResourceManager struct {
	db *sql.DB
}

func RegisterXAResource(db *sql.DB) {
	xaResourceManager = ResourceManager{db: db}
	resource.InitXABranchResource(xaResourceManager)
}

func (resourceManager ResourceManager) Commit(ctx context.Context, bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	_, err := resourceManager.db.ExecContext(ctx, fmt.Sprintf("XA COMMIT '%s'", bs.BranchID))
	if err != nil {
		var mysqlErr *MySQLError
		if errors.As(err, &mysqlErr) {
			if mysqlErr.Number == 1399 {
				return api.Complete, nil
			}
		}
		return bs.Status, err
	}
	return api.Complete, nil
}

func (resourceManager ResourceManager) Rollback(ctx context.Context, bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	_, err := resourceManager.db.ExecContext(ctx, fmt.Sprintf("XA ROLLBACK '%s'", bs.BranchID))
	if err != nil {
		var mysqlErr *MySQLError
		if errors.As(err, &mysqlErr) {
			if mysqlErr.Number == 1399 {
				return api.Complete, nil
			}
		}
		return bs.Status, err
	}
	return api.Complete, nil
}
