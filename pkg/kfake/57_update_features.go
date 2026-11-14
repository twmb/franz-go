package kfake

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// UpdateFeatures: v0-2
//
// KIP-584: mutate finalized feature levels. The features and their supported
// ranges are the kversion table for the cluster's version; kfake stores the
// levels set per Cluster and ApiVersions reports them over the table's
// defaults. Changing a level does not alter request behavior beyond what
// ApiVersions advertises.
//
// Behavior, as Kafka 4.0+:
// * All or nothing: the first invalid update fails the request with a
//   top-level INVALID_UPDATE_VERSION and changes nothing
// * Invalid: an unknown feature, a level outside the supported range or
//   below 0, an UpgradeType other than 1-3, a downgrade with UpgradeType 1
//   (upgrade), or an upgrade with UpgradeType 2 or 3 (downgrade)
// * Level 0 deletes the feature
// * v0 has no UpgradeType, and Kafka reads it as 1, so a v0 downgrade fails
// * The last update for a feature wins
// * ValidateOnly (v1+) returns what would happen without mutating state
// * v0 and v1 list every feature in Results on success
//
// Version notes:
// * v1: UpgradeType replaces AllowDowngrade, ValidateOnly
// * v2: Results removed (errors reported at top-level only)

func init() { regKey(57, 0, 2) }

func (c *Cluster) handleUpdateFeatures(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.UpdateFeaturesRequest)
	resp := req.ResponseKind().(*kmsg.UpdateFeaturesResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A fault, on the cluster or on any feature, answers the request. A
	// timed-out update still updates if every feature is valid.
	fault := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{})
	if fault != nil && creq.skipsWork(fault) {
		resp.ErrorCode = fault.Code
		return resp, nil
	}

	vs := c.featureVersions()
	supported := make(map[string][2]int16)
	vs.EachSupportedFeature(func(name string, min, max int16) { supported[name] = [2]int16{min, max} })
	finalized := make(map[string]int16)
	vs.EachFinalizedFeature(func(name string, level int16) { finalized[name] = level })
	for name, level := range c.features {
		finalized[name] = level
	}

	type update struct {
		level       int16
		upgradeType int8
	}
	var names []string
	updates := make(map[string]update, len(req.FeatureUpdates))
	for _, fu := range req.FeatureUpdates {
		if _, ok := updates[fu.Feature]; !ok {
			names = append(names, fu.Feature)
		}
		ut := fu.UpgradeType
		if req.Version == 0 {
			ut = 1
		}
		updates[fu.Feature] = update{fu.MaxVersionLevel, ut}
	}

	var (
		invalid string
		pending = make(map[string]int16, len(names))
	)
	for _, name := range names {
		if e := creq.faults.check(faultKey{resource: name}); e != nil && fault == nil {
			fault = e
		}
		u := updates[name]
		r, known := supported[name]
		cur := finalized[name]
		switch {
		case u.upgradeType < 1 || u.upgradeType > 3:
			invalid = "The controller does not support the given upgrade type."
		case u.level < 0:
			invalid = "A feature version cannot be less than 0."
		case !known:
			invalid = "unknown feature"
		case u.level < r[0] || u.level > r[1]:
			invalid = "level outside the supported range"
		case u.level < cur && u.upgradeType == 1:
			invalid = "Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade."
		case u.level > cur && u.upgradeType != 1:
			invalid = "Can't downgrade to a newer version."
		}
		if invalid != "" {
			invalid = "Invalid update version " + strconv.Itoa(int(u.level)) + " for feature " + name + ". " + invalid
			break
		}
		pending[name] = u.level
	}

	switch {
	case fault != nil:
		resp.ErrorCode = fault.Code
		if creq.skipsWork(fault) || invalid != "" {
			return resp, nil
		}
	case invalid != "":
		resp.ErrorCode = kerr.InvalidUpdateVersion.Code
		resp.ErrorMessage = kmsg.StringPtr("The update failed for all features since the following feature had an error: " + invalid)
		return resp, nil
	}

	if !(req.Version >= 1 && req.ValidateOnly) {
		var changed bool
		for name, level := range pending {
			changed = changed || finalized[name] != level
			c.features[name] = level
		}
		if changed {
			c.featuresEpoch++
			c.persistDynamicConfigsState()
		}
	}
	if req.Version <= 1 && resp.ErrorCode == 0 {
		for _, name := range names {
			r := kmsg.NewUpdateFeaturesResponseResult()
			r.Feature = name
			resp.Results = append(resp.Results, r)
		}
	}
	return resp, nil
}
