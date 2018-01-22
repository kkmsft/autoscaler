/*
 Copyright 2017 The Kubernetes Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package acs

import (
	"fmt"

	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"	
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

const (
	minAcsAgentNodes = 1
	maxAcsAgentNodes = 100
)

//AgentPool implements Nodegroup
type AgentPool struct {
	name   string
	acsMgr *AcsManager
}

func (agentPool *AgentPool) MaxSize() int {
	return agentPool.acsMgr.MaxSize()
}

func (agentPool *AgentPool) MinSize() int {
	return agentPool.acsMgr.MinSize()
}

// Undelying size of nodes
func (agentPool *AgentPool) TargetSize() (int, error) {
	return agentPool.acsMgr.GetSize()
}

func (agentPool *AgentPool) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("Size increase must be +ve")
	}
	currentSize, err := agentPool.acsMgr.GetSize()
	if err != nil {
		return err
	}
	targetSize := int(currentSize) + delta
	if targetSize > agentPool.MaxSize() {
		return fmt.Errorf("Size increase request of %d more than max size %d set", targetSize, agentPool.MaxSize)
	}
	return agentPool.acsMgr.SetSize(targetSize)
}

func (agentPool *AgentPool) DeleteNodes(nodes []*apiv1.Node) error {
	var providerIDs []string
	for _, node := range nodes {
		glog.Infof("Node: %v", node)
		glog.Infof("Node: %s", node.Spec.ProviderID)
		providerIDs = append(providerIDs, node.Spec.ProviderID)
	}
	for _, p := range providerIDs {
		glog.Infof("ProviderID before calling acsmgr: %s", p)
	}
	return agentPool.acsMgr.DeleteNodes(providerIDs)
}

func (agentPool *AgentPool) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		return fmt.Errorf("Size decrease must be negative")
	}
	currentSize, err := agentPool.acsMgr.GetSize()
	if err != nil {
		return err
	}
	// Get the current nodes in the list
	nodes, err := agentPool.acsMgr.GetNodes()
	if err != nil {
		return err
	}
	targetSize := int(currentSize) + delta
	if targetSize < len(nodes) {
		return fmt.Errorf("Attempt to decrease the size to:%d by reducing by: %d, which is below existing number of nodes: %d",
			targetSize, delta, len(nodes))
	}
	return agentPool.acsMgr.SetSize(targetSize)
}

func (agentPool *AgentPool) Id() string {
	return agentPool.name
}

func (agentPool *AgentPool) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", agentPool.Id(), agentPool.MinSize(), agentPool.MaxSize())
}

func (agentPool *AgentPool) Nodes() ([]string, error) {
	return agentPool.acsMgr.GetNodes()
}

func (agentPool *AgentPool) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (agentPool *AgentPool) Exist() bool {
	return true
}

func (agentPool *AgentPool) Create() error {
	return cloudprovider.ErrAlreadyExist
}

func (agentPool *AgentPool) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (agentPool *AgentPool) Autoprovisioned() bool {
	return false
}

type AcsCloudProvider struct {
	acsManager      *AcsManager
	agentPool       []*AgentPool
	resourceLimiter *cloudprovider.ResourceLimiter
}

func BuildAcsCloudProvider(acsMgr *AcsManager, specs []string, resourceLimiter *cloudprovider.ResourceLimiter) (*AcsCloudProvider, error) {

	agentPool, err := acsMgr.CreateAgentPool()
	if err != nil {
		return nil, err
	}
	acs := &AcsCloudProvider{
		acsManager:      acsMgr,
		agentPool:       agentPool,
		resourceLimiter: resourceLimiter,
	}

	// TODO: Get the node spec and the cluster name from commandline ??
	return acs, nil
}

func (acs *AcsCloudProvider) Name() string {
	return acs.acsManager.ServiceType
}

func (acs *AcsCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	// We support only one group as of now. This maps to the acs cluster hosting k8s
	res := make([]cloudprovider.NodeGroup, 0, len(acs.agentPool))
	for _, node := range acs.agentPool {
		res = append(res, node)
	}
	return res
}

func (acs *AcsCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	// TODO: Covert this to real search.
	return acs.agentPool[0], nil
}

func (acs *AcsCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

func (acs *AcsCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

func (acs *AcsCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (acs *AcsCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return acs.resourceLimiter, nil
}

func (acs *AcsCloudProvider) Cleanup() error {
	acs.acsManager.Cleanup()
	return nil
}

func (acs *AcsCloudProvider) Refresh() error {
	return nil
}
