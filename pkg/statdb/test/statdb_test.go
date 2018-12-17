// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package statdb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"storj.io/storj/internal/testcontext"
	"storj.io/storj/internal/teststorj"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/statdb"
	"storj.io/storj/pkg/storj"
	"storj.io/storj/satellite/satellitedb"
	"storj.io/storj/satellite/satellitedb/satellitedbtest"
)

var (
	nodeID = teststorj.NodeIDFromString("testnodeid")
)

func getRatio(s, t int) (success, total int64, ratio float64) {
	ratio = float64(s) / float64(t)
	return int64(s), int64(t), ratio
}

func TestStatdb(t *testing.T) {
	satellitedbtest.Run(t, func(t *testing.T, db *satellitedb.DB) {
		ctx := testcontext.New(t)
		defer ctx.Cleanup()

		testDatabase(ctx, t, db.StatDB())
	})
}

func testDatabase(ctx context.Context, t *testing.T, sdb statdb.DB) {
	t.Run("TestCreateNewAndWithStats", func(t *testing.T) {
		auditSuccessCount, auditCount, auditSuccessRatio := getRatio(4, 10)
		uptimeSuccessCount, uptimeCount, uptimeRatio := getRatio(8, 25)
		nodeStats := &pb.NodeStats{
			AuditSuccessRatio:  auditSuccessRatio,
			UptimeRatio:        uptimeRatio,
			AuditCount:         auditCount,
			AuditSuccessCount:  auditSuccessCount,
			UptimeCount:        uptimeCount,
			UptimeSuccessCount: uptimeSuccessCount,
		}

		s, err := sdb.Create(ctx, nodeID, nodeStats)
		assert.NoError(t, err)
		assert.EqualValues(t, auditSuccessRatio, s.AuditSuccessRatio)
		assert.EqualValues(t, uptimeRatio, s.UptimeRatio)

		s, err = sdb.Get(ctx, nodeID)
		assert.NoError(t, err)

		assert.EqualValues(t, nodeID, s.NodeId)
		assert.EqualValues(t, auditCount, s.AuditCount)
		assert.EqualValues(t, auditSuccessCount, s.AuditSuccessCount)
		assert.EqualValues(t, auditSuccessRatio, s.AuditSuccessRatio)
		assert.EqualValues(t, uptimeCount, s.UptimeCount)
		assert.EqualValues(t, uptimeSuccessCount, s.UptimeSuccessCount)
		assert.EqualValues(t, uptimeRatio, s.UptimeRatio)
	})

	t.Run("TestCreateExists", func(t *testing.T) {
		auditSuccessCount, auditCount, auditSuccessRatio := getRatio(4, 10)
		uptimeSuccessCount, uptimeCount, uptimeRatio := getRatio(8, 25)
		nodeStats := &pb.NodeStats{
			AuditSuccessRatio:  auditSuccessRatio,
			UptimeRatio:        uptimeRatio,
			AuditCount:         auditCount,
			AuditSuccessCount:  auditSuccessCount,
			UptimeCount:        uptimeCount,
			UptimeSuccessCount: uptimeSuccessCount,
		}
		_, err := sdb.Create(ctx, nodeID, nodeStats)
		assert.Error(t, err)
	})

	t.Run("TestGetDoesNotExist", func(t *testing.T) {
		noNodeID := teststorj.NodeIDFromString("testnoNodeid")

		_, err := sdb.Get(ctx, noNodeID)
		assert.Error(t, err)
	})

	t.Run("TestFindInvalidNodes", func(t *testing.T) {
		invalidNodeIDs := teststorj.NodeIDsFromStrings("id1", "id2", "id3", "id4", "id5", "id6", "id7")
		for _, tt := range []struct {
			nodeID             storj.NodeID
			auditSuccessCount  int64
			auditCount         int64
			auditSuccessRatio  float64
			uptimeSuccessCount int64
			uptimeCount        int64
			uptimeRatio        float64
		}{
			{invalidNodeIDs[0], 20, 20, 1, 20, 20, 1},   // good audit success
			{invalidNodeIDs[1], 5, 20, 0.25, 20, 20, 1}, // bad audit success, good uptime
			{invalidNodeIDs[2], 20, 20, 1, 5, 20, 0.25}, // good audit success, bad uptime
			{invalidNodeIDs[3], 0, 0, 0, 20, 20, 1},     // "bad" audit success, no audits
			{invalidNodeIDs[4], 20, 20, 1, 0, 0, 0.25},  // "bad" uptime success, no checks
			{invalidNodeIDs[5], 0, 1, 0, 5, 5, 1},       // bad audit success exactly one audit
			{invalidNodeIDs[6], 0, 20, 0, 20, 20, 1},    // bad ratios, excluded from query
		} {
			nodeStats := &pb.NodeStats{
				AuditSuccessRatio:  tt.auditSuccessRatio,
				UptimeRatio:        tt.uptimeRatio,
				AuditCount:         tt.auditCount,
				AuditSuccessCount:  tt.auditSuccessCount,
				UptimeCount:        tt.uptimeCount,
				UptimeSuccessCount: tt.uptimeSuccessCount,
			}

			_, err := sdb.Create(ctx, tt.nodeID, nodeStats)
			assert.NoError(t, err)
		}

		nodeIds := storj.NodeIDList{
			invalidNodeIDs[0], invalidNodeIDs[1],
			invalidNodeIDs[2], invalidNodeIDs[3],
			invalidNodeIDs[4], invalidNodeIDs[5],
		}
		maxStats := &pb.NodeStats{
			AuditSuccessRatio: 0.5,
			UptimeRatio:       0.5,
		}

		invalid, err := sdb.FindInvalidNodes(ctx, nodeIds, maxStats)
		assert.NoError(t, err)

		assert.Contains(t, invalid, invalidNodeIDs[1])
		assert.Contains(t, invalid, invalidNodeIDs[2])
		assert.Contains(t, invalid, invalidNodeIDs[5])
		assert.Len(t, invalid, 3)
	})

	t.Run("TestUpdateExists", func(t *testing.T) {
		auditSuccessCount, auditCount, auditSuccessRatio := getRatio(4, 10)
		uptimeSuccessCount, uptimeCount, uptimeRatio := getRatio(8, 25)

		s, err := sdb.Get(ctx, nodeID)
		assert.NoError(t, err)

		assert.EqualValues(t, nodeID, s.NodeId)
		assert.EqualValues(t, auditCount, s.AuditCount)
		assert.EqualValues(t, auditSuccessCount, s.AuditSuccessCount)
		assert.EqualValues(t, auditSuccessRatio, s.AuditSuccessRatio)
		assert.EqualValues(t, uptimeCount, s.UptimeCount)
		assert.EqualValues(t, uptimeSuccessCount, s.UptimeSuccessCount)
		assert.EqualValues(t, uptimeRatio, s.UptimeRatio)

		updateReq := &statdb.UpdateRequest{
			AuditSuccess: true,
			IsUp:         false,
		}
		stats, err := sdb.Update(ctx, updateReq)
		assert.NoError(t, err)

		_, _, newAuditRatio := getRatio(int(auditSuccessCount+1), int(auditCount+1))
		_, _, newUptimeRatio := getRatio(int(uptimeSuccessCount), int(uptimeCount+1))

		assert.EqualValues(t, newAuditRatio, stats.AuditSuccessRatio)
		assert.EqualValues(t, newUptimeRatio, stats.UptimeRatio)
	})

	t.Run("TestUpdateUptimeExists", func(t *testing.T) {
		auditSuccessCount, auditCount, auditSuccessRatio := getRatio(5, 11)
		uptimeSuccessCount, uptimeCount, uptimeRatio := getRatio(8, 26)

		s, err := sdb.Get(ctx, nodeID)
		assert.NoError(t, err)

		assert.EqualValues(t, nodeID, s.NodeId)
		assert.EqualValues(t, auditCount, s.AuditCount)
		assert.EqualValues(t, auditSuccessCount, s.AuditSuccessCount)
		assert.EqualValues(t, auditSuccessRatio, s.AuditSuccessRatio)
		assert.EqualValues(t, uptimeCount, s.UptimeCount)
		assert.EqualValues(t, uptimeSuccessCount, s.UptimeSuccessCount)
		assert.EqualValues(t, uptimeRatio, s.UptimeRatio)

		stats, err := sdb.UpdateUptime(ctx, nodeID, false)
		assert.NoError(t, err)

		_, _, newUptimeRatio := getRatio(int(uptimeSuccessCount), int(uptimeCount+1))
		assert.EqualValues(t, auditSuccessRatio, stats.AuditSuccessRatio)
		assert.EqualValues(t, auditCount, stats.AuditCount)
		assert.EqualValues(t, newUptimeRatio, stats.UptimeRatio)
	})

	t.Run("TestUpdateAuditSuccessExists", func(t *testing.T) {
		auditSuccessCount, auditCount, auditSuccessRatio := getRatio(5, 11)
		uptimeSuccessCount, uptimeCount, uptimeRatio := getRatio(8, 27)

		stats, err := sdb.Get(ctx, nodeID)
		assert.NoError(t, err)

		assert.EqualValues(t, nodeID, stats.NodeId)
		assert.EqualValues(t, auditCount, stats.AuditCount)
		assert.EqualValues(t, auditSuccessCount, stats.AuditSuccessCount)
		assert.EqualValues(t, auditSuccessRatio, stats.AuditSuccessRatio)
		assert.EqualValues(t, uptimeCount, stats.UptimeCount)
		assert.EqualValues(t, uptimeSuccessCount, stats.UptimeSuccessCount)
		assert.EqualValues(t, uptimeRatio, stats.UptimeRatio)

		stats, err = sdb.UpdateAuditSuccess(ctx, nodeID, false)
		assert.NoError(t, err)

		_, _, newAuditRatio := getRatio(int(auditSuccessCount), int(auditCount+1))
		assert.EqualValues(t, newAuditRatio, stats.AuditSuccessRatio)
		assert.EqualValues(t, auditCount+1, stats.AuditCount)
		assert.EqualValues(t, uptimeRatio, stats.UptimeRatio)
	})

	t.Run("TestUpdateBatchExists", func(t *testing.T) {
		nodeID1 := teststorj.NodeIDFromString("testnodeid1")
		auditSuccessCount1, auditCount1, auditRatio1 := getRatio(4, 10)
		uptimeSuccessCount1, uptimeCount1, uptimeRatio1 := getRatio(8, 25)
		nodeStats := &pb.NodeStats{
			AuditSuccessCount:  auditSuccessCount1,
			AuditCount:         auditCount1,
			AuditSuccessRatio:  auditRatio1,
			UptimeSuccessCount: uptimeSuccessCount1,
			UptimeCount:        uptimeCount1,
			UptimeRatio:        uptimeRatio1,
		}

		s, err := sdb.Create(ctx, nodeID1, nodeStats)
		assert.NoError(t, err)
		assert.EqualValues(t, auditRatio1, s.AuditSuccessRatio)
		assert.EqualValues(t, uptimeRatio1, s.UptimeRatio)

		nodeID2 := teststorj.NodeIDFromString("testnodeid2")
		auditSuccessCount2, auditCount2, auditRatio2 := getRatio(7, 10)
		uptimeSuccessCount2, uptimeCount2, uptimeRatio2 := getRatio(8, 20)
		nodeStats = &pb.NodeStats{
			AuditSuccessCount:  auditSuccessCount2,
			AuditCount:         auditCount2,
			AuditSuccessRatio:  auditRatio2,
			UptimeSuccessCount: uptimeSuccessCount2,
			UptimeCount:        uptimeCount2,
			UptimeRatio:        uptimeRatio2,
		}

		s, err = sdb.Create(ctx, nodeID2, nodeStats)
		assert.NoError(t, err)
		assert.EqualValues(t, auditRatio2, s.AuditSuccessRatio)
		assert.EqualValues(t, uptimeRatio2, s.UptimeRatio)

		updateReqList := []*statdb.UpdateRequest{
			&statdb.UpdateRequest{
				NodeID:       nodeID1,
				AuditSuccess: true,
				IsUp:         false,
			},
			&statdb.UpdateRequest{
				NodeID:       nodeID2,
				AuditSuccess: true,
				IsUp:         true,
			},
		}
		statsList, _, err := sdb.UpdateBatch(ctx, updateReqList)
		assert.NoError(t, err)

		_, _, newAuditRatio1 := getRatio(int(auditSuccessCount1+1), int(auditCount1+1))
		_, _, newUptimeRatio1 := getRatio(int(uptimeSuccessCount1), int(uptimeCount1+1))
		_, _, newAuditRatio2 := getRatio(int(auditSuccessCount2+1), int(auditCount2+1))
		stats1 := statsList[0]
		stats2 := statsList[1]
		assert.EqualValues(t, newAuditRatio1, stats1.AuditSuccessRatio)
		assert.EqualValues(t, newUptimeRatio1, stats1.UptimeRatio)
		assert.EqualValues(t, newAuditRatio2, stats2.AuditSuccessRatio)
		assert.EqualValues(t, uptimeRatio2, stats2.UptimeRatio)
	})
}
