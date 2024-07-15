package coscheduling

import (
	"context"
	"errors"
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

var ErrSkipZone = errors.New("skipping strict-zone selection")

type podGroupZone struct {
	zone string
	skip bool
}

func (p *podGroupZone) Clone() framework.StateData {
	c := *p
	return &c
}

func (cs *Coscheduling) selectZone(ctx context.Context, state *framework.CycleState, pod *v1.Pod, groupLabel string) (string, error) {
	resName, ok := pod.Annotations[v1alpha1.PodGroupAnnotationGroupResource]
	if !ok {
		// TODO: Fail or skip on grouping?
		return "", fmt.Errorf("%w: pod requested grouping, but did not provide resource", ErrSkipZone)
	}

	pgName := util.GetPodGroupLabel(pod)

	// TODO: required resources

	// Site affinity.. too specific to us?
	//
	var selectedZone string
	// Get from cache by pgname

	if selectedZone != "" {
		return selectedZone, nil
	}

	zones, err := cs.zones(*pod.Spec.Priority, resName, resValue, pod.Annotations)
	if err != nil {
		return "", nil
	}

	// Get zones
	if klog.V(5).Enabled() {
		for name, info := range zones {
			klog.InfoS(
				"Zone resource information",
				"zone", name,
				"free_resources", info.freeResources,
				"resources_preempt", info.toPreempt,
				"podgroup", pgName,
			)
		}
	}

	// Get total requested resources of group. Check minResource val in spec first
	totalResources, err := cs.totalGroupRequest(ctx, pod)
	if err != nil {
		return "", err
	}
	klog.V(4).Infof("Total requested resources for %s: %d", pgName, totalResources)

	// Order the zones by least destructive behavior for the pod's case.
	var orderedZones []string
	if *pod.Spec.Priority > 0 {
		orderedZones = zoneLessPreempt(zones)
	} else {
		orderedZones = zoneFreeAsc(zones)
	}

	// Check if user provided specific zones, then filter the list by his choice.
	userReqZone, ok := cs.userZoneAffinity(pod)
	if ok {
		orderedZones = intersection(orderedZones, userReqZone)
	}

	for _, zone := range orderedZones {
		klog.Infof("Checking zone for free sources", "zone", zone, "free_resources", zones[zone].freeResources)
		if zones[zone].freeResources >= totalResources {
			klog.InfoS("Found zone", "podgroup", pgName, "zone", zone)
			return zone, nil
		}
	}

	return "", fmt.Errorf("no zone can fulfill the request")
}

type zoneInfo struct {
	freeResources int64
	toPreempt     int64
}

func (cs *Coscheduling) zones(priority int32, resName string, resVal int64, requiredResources int, annotations map[string]string) (map[string]zoneInfo, error) {
	zones := make(map[string]zoneInfo)

	groupBy := annotations[v1alpha1.PodGroupAnnotationGroupBy]
	excludeSelector := annotations[v1alpha1.PodGroupAnnotationExclude]

	nl, err := cs.frameworkHandler.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fmt.Errorf("zones: listing nodes: %w", err)
	}

	reqs, err := labels.ParseToRequirements(excludeSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid exclude annotations: %w", err)
	}

	for _, n := range nl {
		// TODO: filter by exlude label
		if isExcluded(n, reqs) || !isReady(n) {
			klog.V(5).InfoS("node excluded", "node", n.Node().Name, "reason", "not ready or excluded by selector")
			continue
		}

		nodeZone, ok := n.Node().Labels[groupBy]
		if !ok {
			klog.V(4).InfoS("node excluded", "node", n.Node().Name, "reason", "node does not have the required group label")
			continue
		}

		// Calculate free resources, and potential preemption if needed.
		resFree, toPreempt := cs.freeResources(n, priority, v1.ResourceName(resName))

		// Multi-HLS requested are expected to be with full-boxes, so we add only nodes
		// that can potentially hold 8-cards pods.
		// TODO: compare to pod request, than calculate if there are enough nodes for min member
		if resFree < int64(requiredResources) {
			klog.V(4).Infof("node %s [%d] cannot serve pod for %d cards", n.Node().Name, resFree, requiredResources)
			continue
		}

		zoneVal := zones[nodeZone]
		zoneVal.freeResources += resFree
		zoneVal.toPreempt += int64(toPreempt)
		zones[nodeZone] = zoneVal
	}

	return zones, nil
}

func isExcluded(node *framework.NodeInfo, reqs []labels.Requirement) bool {
	for _, req := range reqs {
		if req.Matches(labels.Set(node.Node().Labels)) {
			return true
		}
	}
	return false
}

func isReady(node *framework.NodeInfo) bool {
	for _, cond := range node.Node().Status.Conditions {
		if cond.Status == v1.ConditionTrue && cond.Type == v1.NodeReady {
			return true
		}
	}
	return false
}

// zoneFreeAsc sort zone by the number of free cards in ascending order
func zoneFreeAsc(zones map[string]zoneInfo) []string {
	keys := make([]string, 0, len(zones))

	for k := range zones {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return zones[keys[i]].freeResources < zones[keys[j]].freeResources
	})

	return keys
}

func zoneLessPreempt(zones map[string]zoneInfo) []string {
	keys := make([]string, 0, len(zones))

	for k := range zones {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return (zones[keys[i]].toPreempt < zones[keys[j]].toPreempt)
	})

	return keys
}

func (cs *Coscheduling) totalGroupRequest(ctx context.Context, pod *v1.Pod) (int64, error) {
	resName, ok := pod.Annotations[v1alpha1.PodGroupAnnotationGroupResource]
	if !ok {
		// TODO: Fail or skip on grouping?
		return 0, fmt.Errorf("%w: pod requested grouping, but did not provide resource", ErrSkipZone)
	}

	// Check if podgroup has the minResource configured in spec
	pgName, pg := cs.pgMgr.GetPodGroup(ctx, pod)
	if pg == nil {
		return 0, fmt.Errorf("pod group not found in manager")
	}

	if pg.Spec.MinResources != nil {
		// If the minResources is configured in spec, we'll take the value
		// of the requested resource and return early.
		resVal, ok := pg.Spec.MinResources[v1.ResourceName(resName)]
		if ok {
			return resVal.Value(), nil
		}
	}

	// Extract the requested resource value from the containers
	var resValue int64
	for _, c := range pod.Spec.Containers {
		if val, ok := c.Resources.Requests[v1.ResourceName(resName)]; ok {
			resValue = val.Value()
			break
		}
	}

	// Find all members of the group
	pgReq, err := labels.NewRequirement(v1alpha1.PodGroupLabel, selection.Equals, []string{pgName})
	if err != nil {
		return 0, err
	}
	selector := labels.NewSelector().Add(*pgReq)
	pods, err := cs.frameworkHandler.SharedInformerFactory().Core().V1().Pods().Lister().List(selector)
	if err != nil {
		return 0, err
	}
	// TODO: Filter out pods without the req annotation, as it is expected only
	// on pods that actually

	return int64(len(pods)) * resValue, nil
}

func (cs *Coscheduling) freeResources(node *framework.NodeInfo, priority int32, resName v1.ResourceName) (int64, int) {
	allocatable := node.Allocatable.ScalarResources[resName]

	// TODO: do we care about specific pods or just those who has the resources?
	var podsOnNode []*framework.PodInfo
	for _, p := range node.Pods {
		if hasRequestedResource(p, resName) {
			podsOnNode = append(podsOnNode, p)
		}
	}

	inUse := node.Requested.ScalarResources[resName]
	if priority == 0 {
		return allocatable - inUse, 0
	}

	var potentialCards int64
	var toPreempt int
	for _, p := range podsOnNode {
		if priority > *p.Pod.Spec.Priority {
			podCards := p.Pod.Spec.Containers[0].Resources.Requests[resName]
			potentialCards += podCards.Value()
			toPreempt++
		}
	}

	return (allocatable - inUse) + potentialCards, toPreempt
}

func hasRequestedResource(pod *framework.PodInfo, resName v1.ResourceName) bool {
	for _, c := range pod.Pod.Spec.Containers {
		if _, ok := c.Resources.Requests[resName]; ok {
			return true
		}
	}
	return false
}
