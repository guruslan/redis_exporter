package exporter

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
)

type sentinelDiscoveryNode struct {
	address string
	role    string
}

func (e *Exporter) getSentinelNodes(c redis.Conn, masterName string) ([]sentinelDiscoveryNode, error) {
	masterValues, err := redis.Values(doRedisCmd(c, "SENTINEL", "MASTER", masterName))
	if err != nil {
		log.Errorf("Error getting sentinel master details for %s: %v", masterName, err)
		return nil, fmt.Errorf("sentinel master query failed: %w", err)
	}

	masterDetail, err := redis.StringMap(masterValues, nil)
	if err != nil {
		log.Errorf("Error parsing sentinel master details for %s: %v", masterName, err)
		return nil, fmt.Errorf("invalid sentinel master data: %w", err)
	}

	nodes := make([]sentinelDiscoveryNode, 0, 1)
	if addr, ok := sentinelNodeAddress(masterDetail); ok {
		nodes = append(nodes, sentinelDiscoveryNode{address: addr, role: "master"})
	}

	replicaDetails, err := sentinelReplicaDetails(c, masterName)
	if err != nil {
		log.Errorf("Error getting sentinel replica details for %s: %v", masterName, err)
		return nil, err
	}

	for _, replicaDetail := range replicaDetails {
		if flags, ok := replicaDetail["flags"]; ok {
			if strings.Contains(flags, "o_down") || strings.Contains(flags, "s_down") || strings.Contains(flags, "disconnected") {
				continue
			}
		}

		addr, ok := sentinelNodeAddress(replicaDetail)
		if !ok {
			continue
		}

		nodes = append(nodes, sentinelDiscoveryNode{address: addr, role: "replica"})
	}

	return nodes, nil
}

func sentinelNodeAddress(details map[string]string) (string, bool) {
	ip, ipOK := details["ip"]
	port, portOK := details["port"]
	if ipOK && portOK && ip != "" && ip != "?" && port != "" {
		return ip + ":" + port, true
	}

	if addr, ok := details["addr"]; ok && addr != "" {
		return addr, true
	}
	if addr, ok := details["address"]; ok && addr != "" {
		return addr, true
	}

	return "", false
}

func sentinelReplicaDetails(c redis.Conn, masterName string) ([]map[string]string, error) {
	values, err := redis.Values(doRedisCmd(c, "SENTINEL", "REPLICAS", masterName))
	if err != nil {
		if !isSentinelReplicaUnsupported(err) {
			return nil, fmt.Errorf("sentinel replica query failed: %w", err)
		}

		values, err = redis.Values(doRedisCmd(c, "SENTINEL", "SLAVES", masterName))
		if err != nil {
			return nil, fmt.Errorf("sentinel replica query failed: %w", err)
		}
	}

	replicas := make([]map[string]string, 0, len(values))
	for _, value := range values {
		detail, err := redis.StringMap(value, nil)
		if err != nil {
			log.Debugf("Error parsing sentinel replica detail: %v", err)
			continue
		}
		replicas = append(replicas, detail)
	}

	return replicas, nil
}

func isSentinelReplicaUnsupported(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, redis.ErrNil) {
		return false
	}

	msg := err.Error()
	return strings.Contains(strings.ToLower(msg), "unknown command") || strings.Contains(strings.ToLower(msg), "unknown subcommand")
}
