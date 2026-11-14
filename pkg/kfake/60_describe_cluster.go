package kfake

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeCluster: v0-2
//
// KIP-700: Admin API to describe cluster.
//
// Version notes:
// * v1: EndpointType
// * v2: IncludeFencedBrokers, IsFenced

func init() { regKey(60, 0, 2) }

func (c *Cluster) handleDescribeCluster(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DescribeClusterRequest)
	resp := req.ResponseKind().(*kmsg.DescribeClusterResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// We are brokers (endpoint type 1). A request for controllers (2), or
	// with any other type, fails as on Kafka.
	switch req.EndpointType {
	case 1:
	case 2:
		resp.ErrorCode = kerr.MismatchedEndpointType.Code
		resp.ErrorMessage = kmsg.StringPtr("The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER")
		return resp, nil
	default:
		resp.ErrorCode = kerr.UnsupportedEndpointType.Code
		resp.ErrorMessage = kmsg.StringPtr("Unsupported endpoint type " + strconv.Itoa(int(req.EndpointType)))
		return resp, nil
	}

	for _, b := range c.bs {
		sb := kmsg.NewDescribeClusterResponseBroker()
		sb.NodeID = b.node
		sb.Host, sb.Port = b.hostport()
		sb.Rack = &brokerRack
		resp.Brokers = append(resp.Brokers, sb)
	}

	resp.ClusterID = c.cfg.clusterID
	resp.ControllerID = c.controller.node

	if req.IncludeClusterAuthorizedOperations {
		resp.ClusterAuthorizedOperations = c.clusterAuthorizedOps(creq)
	}

	return resp, nil
}
