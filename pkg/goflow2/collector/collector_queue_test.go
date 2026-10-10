package collector

import (
	"encoding/json"
	"testing"

	"github.com/netsampler/goflow2/v3/utils"
	"github.com/stretchr/testify/require"
)

func TestCollectorQueueStats(t *testing.T) {
	recv, err := utils.NewUDPReceiver(&utils.UDPReceiverConfig{QueueSize: 4})
	require.NoError(t, err)

	c := &Collector{
		queueReporters: []queueReporter{
			{scheme: "netflow", hostname: "127.0.0.1", port: 2055, recv: recv},
		},
	}

	stats := c.QueueStats()
	require.Len(t, stats, 1)
	require.Equal(t, "netflow", stats[0].Scheme)
	require.Equal(t, "127.0.0.1", stats[0].Hostname)
	require.Equal(t, 2055, stats[0].Port)
	require.Equal(t, 4, stats[0].Capacity)
	require.Equal(t, 0, stats[0].Depth)
	require.Equal(t, 0.0, stats[0].Utilization)

	b, err := json.Marshal(stats)
	require.NoError(t, err)
	require.JSONEq(t, `[{"scheme":"netflow","hostname":"127.0.0.1","port":2055,"depth":0,"capacity":4,"utilization":0}]`, string(b))
}

func TestCollectorQueueStatsEmpty(t *testing.T) {
	c := &Collector{}
	stats := c.QueueStats()
	require.NotNil(t, stats)
	require.Len(t, stats, 0)
}
