package coscheduling

import (
	"context"
	"errors"
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/coscheduling/core"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

var ErrSkipZone = errors.New("skipping strict-zone selection")

func (cs *Coscheduling) selectZone(ctx context.Context, _ *framework.CycleState, pod *v1.Pod) (string, error) {
	resourceName, ok := pod.Annotations[v1alpha1.PodGroupAnnotationGroupResource]
	if !ok {
		// TODO: Fail or skip on grouping?
		return "", fmt.Errorf("%w: pod requested grouping, but did not provide resource", ErrSkipZone)
	}

	pgName := util.GetPodGroupLabel(pod)

	// TODO: required resources

	// Check in cache if the group has already have a zone.
	zoneData := cs.pgMgr.GetPodGroupZone(ctx, pgName)
	if zoneData != nil {
		klog.V(4).InfoS("zone found in cache", "podgroup", pgName, "zone", zoneData.Zone)
		return zoneData.Zone, nil
	}

	zones, err := cs.zones(pod, resourceName)
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
	klog.V(4).InfoS("Total requested resources", "podgroup", pgName, "resource", resourceName, "total_requested", totalResources)

	// Order the zones by least destructive behavior for the pod's case.
	var orderedZones []string
	if *pod.Spec.Priority > 0 {
		orderedZones = zoneLessPreempt(zones)
	} else {
		orderedZones = zoneFreeAsc(zones)
	}

	// Check if user provided specific zones, then filter the list by his choice.
	// userReqZone, ok := cs.userZoneAffinity(pod)
	// if ok {
	// 	orderedZones = intersection(orderedZones, userReqZone)
	// }

	for _, zone := range orderedZones {
		klog.InfoS("Checking zone for free sources", "zone", zone, "free_resources", zones[zone].freeResources)
		if zones[zone].freeResources >= totalResources {
			klog.InfoS("Found zone", "podgroup", pgName, "zone", zone)
			if err := cs.pgMgr.AddPodGroupZone(ctx, pgName, &core.PodGroupZone{
				Zone: zone,
			}); err != nil {
				klog.ErrorS(err, "Adding podgroup to cache", "podgroup", pgName, "pod", pod.Name)
				return "", fmt.Errorf("failed adding podgroup to cache: %w", err)
			}

			return zone, nil
		}
	}

	return "", fmt.Errorf("no zone can fulfill the request")
}

type zoneInfo struct {
	freeResources int64
	toPreempt     int64
}

func (cs *Coscheduling) zones(pod *v1.Pod, resName string) (map[string]zoneInfo, error) {
	priority := *pod.Spec.Priority

	zones := make(map[string]zoneInfo)

	groupBy := pod.Annotations[v1alpha1.PodGroupAnnotationGroupBy]
	excludeSelector := pod.Annotations[v1alpha1.PodGroupAnnotationExclude]

	nl, err := cs.frameworkHandler.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fmt.Errorf("zones: listing nodes: %w", err)
	}

	reqs, err := labels.ParseToRequirements(excludeSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid exclude annotations: %w", err)
	}

	podRequestedResource := extractPodResource(pod, resName)

	for _, n := range nl {
		// TODO: filter by exlude label
		if !matchAffinity(pod, n.Node()) || isExcluded(n, reqs) || !isReady(n) {
			klog.V(5).InfoS("node excluded", "podgroup", util.GetPodGroupLabel(pod), "node", n.Node().Name, "reason", "not ready or excluded by selector")
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
		if resFree < int64(podRequestedResource) {
			klog.V(4).InfoS("node exluded", "node", n.Node().Name,
				"reason", fmt.Sprintf("not enough resources: requested: %d, free %d", podRequestedResource, resFree))
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
	_, pg := cs.pgMgr.GetPodGroup(ctx, pod)
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

	// Find all members of the group
	pgReq, err := labels.NewRequirement(v1alpha1.PodGroupLabel, selection.Equals, []string{pg.Name})
	if err != nil {
		return 0, fmt.Errorf("finding all members of podgroup: %w", err)
	}
	selector := labels.NewSelector().Add(*pgReq)
	pods, err := cs.frameworkHandler.SharedInformerFactory().Core().V1().Pods().Lister().List(selector)
	if err != nil {
		return 0, err
	}

	var totalRes int64
	for _, p := range pods {
		totalRes += extractPodResource(p, resName)
	}

	return totalRes, nil
}

func (cs *Coscheduling) freeResources(node *framework.NodeInfo, priority int32, resName v1.ResourceName) (int64, int) {
	allocatable := node.Allocatable.ScalarResources[resName]

	// TODO: do we care about specific pods or just those who has the resources?
	var podsOnNode []*framework.PodInfo
	for _, p := range node.Pods {
		if hasRequestedResource(p.Pod, resName) {
			klog.V(5).Infof("resources: counting pod: %s", p.Pod.Name)
			podsOnNode = append(podsOnNode, p)
		}
	}

	inUse := node.Requested.ScalarResources[resName]
	klog.V(5).InfoS("calculated resources on node", "node", node.Node().Name, "allocatable", allocatable, "in_use", inUse)
	if priority == 0 || len(podsOnNode) == 0 {
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

func hasRequestedResource(pod *v1.Pod, resName v1.ResourceName) bool {
	for _, c := range pod.Spec.Containers {
		if _, ok := c.Resources.Requests[resName]; ok {
			return true
		}
	}
	return false
}

func extractPodResource(pod *v1.Pod, resourceName string) int64 {
	for _, c := range pod.Spec.Containers {
		if val, ok := c.Resources.Requests[v1.ResourceName(resourceName)]; ok {
			return val.Value()
		}
	}
	return 0
}

func matchAffinity(pod *v1.Pod, node *v1.Node) bool {
	reqRequired := nodeaffinity.GetRequiredNodeAffinity(pod)
	match, err := reqRequired.Match(node)
	if err != nil {
		klog.V(4).ErrorS(err, "matching node by selectors")
		return false
	}
	return match
}
