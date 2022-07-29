// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"strings"
	"time"

	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/hptx/pkg/config"
	"github.com/cectc/hptx/pkg/core"
	"github.com/cectc/hptx/pkg/proto"
	"github.com/pkg/errors"
)

type mysqlTx struct {
	mc *mysqlConn
}

func (tx *mysqlTx) Commit() (err error) {
	defer func() {
		if tx.mc != nil {
			tx.mc.ctx = nil
		}
		tx.mc = nil
	}()

	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}

	if tx.mc.ctx != nil {
		err := tx.register()
		if err != nil {
			rollBackErr := tx.mc.exec("ROLLBACK")
			if rollBackErr != nil {
				log.Error(rollBackErr)
			}
			return err
		}
		if len(tx.mc.ctx.sqlUndoItemsBuffer) > 0 {
			err = GetUndoLogManager().FlushUndoLogs(tx.mc)
			if err != nil {
				reportErr := tx.report(false)
				if reportErr != nil {
					return reportErr
				}
				return err
			}
			err = tx.mc.exec("COMMIT")
			if err != nil {
				reportErr := tx.report(false)
				if reportErr != nil {
					return reportErr
				}
				return err
			}
		} else {
			err = tx.mc.exec("COMMIT")
			return err
		}
	} else {
		err = tx.mc.exec("COMMIT")
		return err
	}
	return
}

func (tx *mysqlTx) Rollback() (err error) {
	defer func() {
		if tx.mc != nil {
			tx.mc.ctx = nil
		}
		tx.mc = nil
	}()

	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}
	err = tx.mc.exec("ROLLBACK")

	if tx.mc.ctx != nil {
		err := tx.register()
		if err != nil {
			return err
		}
		reportErr := tx.report(false)
		if reportErr != nil {
			return reportErr
		}
		return err
	}
	return
}

func (tx *mysqlTx) register() error {
	var (
		branchID        string
		branchSessionID int64
		err             error
		request         = &proto.BranchRegister{
			XID:             tx.mc.ctx.xid,
			ResourceID:      tx.mc.cfg.DBName,
			LockKey:         strings.Join(tx.mc.ctx.lockKeys, ";"),
			BranchType:      api.AT,
			ApplicationData: nil,
		}
	)

	for retryCount := 0; retryCount < config.GetATConfig().LockRetryTimes; retryCount++ {
		branchID, branchSessionID, err = core.GetDistributedTransactionManager().BranchRegister(context.Background(), request)
		if err == nil {
			break
		}
		log.Errorf("branch register err: %v", err)
		time.Sleep(config.GetATConfig().LockRetryInterval)
	}
	tx.mc.ctx.branchID = branchID
	tx.mc.ctx.branchSessionID = branchSessionID
	return err
}

func (tx *mysqlTx) report(commitDone bool) error {
	retry := config.GetATConfig().LockRetryTimes
	for retry > 0 {
		var err error
		if commitDone {
			err = core.GetDistributedTransactionManager().BranchReport(context.Background(),
				tx.mc.ctx.branchID, api.PhaseOneFailed)
		} else {
			err = core.GetDistributedTransactionManager().BranchReport(context.Background(),
				tx.mc.ctx.branchID, api.PhaseOneFailed)
		}
		if err != nil {
			log.Errorf("Failed to report [%d/%s] commit done [%t] Retry Countdown: %d",
				tx.mc.ctx.branchID, tx.mc.ctx.xid, commitDone, retry)
		}
		retry = retry - 1
		if retry == 0 {
			return errors.WithMessagef(err, "Failed to report branch status %t", commitDone)
		}
	}
	return nil
}
