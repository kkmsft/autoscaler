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

package azure

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/compute"
	"github.com/Azure/azure-sdk-for-go/arm/resources/resources"
	azStorage "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
)

// AcsAksAcsAksAgentPool implements NodeGroup interface for agent pools deployes in ACS/AKS
type AcsAksAcsAksAgentPool struct {
	azureRef
	manager *AzureManager

	minSize int
	maxSize int
	containerSvcType string

	template   map[string]interface{}
	parameters map[string]interface{}

	mutex       sync.Mutex
	lastRefresh time.Time
	curSize     int64
}

func NewContainerSvcAgentPool(spec *dynamic.NodeGroupSpec, am *AzureManager) (*AgentPool, error) {
	asg := &AgentPool{
		azureRef: azureRef{
			Name: spec.Name,
		},
		minSize: spec.MinSize,
		maxSize: spec.MaxSize,
		manager: am,
	}
	asg.containerSvcType = am.config.VMType
	return asg, nil
}

func (agentPool *AcsAksAgentPool) MaxSize() int {
	return maxSize
}

func (agentPool *AcsAksAgentPool) MinSize() int {
	return minSize
}

// Undelying size of nodes
func (agentPool *AcsAksAgentPool) TargetSize() (int, error) {
	gotContainerService, err := manager.azClient.ContainerServicesClient.Get(manager.Config.ResourceGroup,
	                                                                         manager.Config.ClusterName)
	if err != nil {
		glog.Error(err)
		return -1, err
	}
	return GetPool(*(gotContainerService.AgentPoolProfiles))
}


func (agentPool *AcsAksAgentPool) GetPool(agentProfiles []AgentPoolProfile) retAPP *AgentPoolProfile
{
	// Keep track if we found the agent pool with given name.
	bool found = true
	for name, value  := range agentProfiles {
		if name == azureRef {
			found := true
			ret_val :=  value.Count
		}
	}
	if found == true {
		return ret_vale
	} else {
		return false
	}
}


func (agentPool *AcsAksAgentPool) SetSize(targetSize int) error {
	glog.Infof("Set size request: %d", targetSize)
	if targetSize > maxSize() || targetSize < minSize() {
		glog.Errorf("Target size %d requested outside Max: %d, Min: %d", targetSize, maxSize, minSize)
		return fmt.Errorf("Target size %d requested outside Max: %d, Min: %d", targetSize, maxSize, minSize)
	}

	gotContainerService, err := acsMgr.ServicesClient.Get(manager.Config.ResourceGroup,
						              manager.Config.AcsClusterName)
	if err != nil {
		glog.Error(err)
		return err
	}
	currentSize := TargetSize()

	glog.Infof("Current size: %d, Target size requested: %d", currentSize, targetSize)
	// Set the value in the volatile structure.
	*((*gotContainerService.AgentPoolProfiles)[0].Count) = int32(targetSize)

	cancel := make(chan struct{})

	var sp containerservice.ServicePrincipalProfile
	sp.ClientID = &(acsMgr.Config.AADClientID)
	sp.Secret = &(acsMgr.Config.AADClientSecret)
	gotContainerService.ServicePrincipalProfile = &sp

	//Update the service with the new value.
	updatedVal, chanErr := acsMgr.ServicesClient.CreateOrUpdate(acsMgr.Config.ResourceGroup, *gotContainerService.Name, gotContainerService, cancel)
	start := time.Now()

	error := <-chanErr
	if error != nil {
		return error
	}
	new_val := <-updatedVal
	end := time.Now()
	glog.Infof("Got Updated value. Time taken: ", end.Sub(start))
	glog.Infof("Target size set done !!. Val: %+v", new_val)
	return nil
}


func (agentPool *AcsAksAgentPool) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("Size increase must be +ve")
	}
	currentSize, err := TargetSize()
	if err != nil {
		return err
	}
	targetSize := int(currentSize) + delta
	if targetSize > agentPool.MaxSize() {
		return fmt.Errorf("Size increase request of %d more than max size %d set", targetSize, agentPool.MaxSize)
	}
	return agentPool.acsMgr.SetSize(targetSize)
}

func (agentPool *AcsAksAgentPool) DeleteNodes(nodes []*apiv1.Node) error {
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

func (agentPool *AcsAksAgentPool) DecreaseTargetSize(delta int) error {
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





func (agentPool *AcsAksAgentPool) Id() string {
	return agentPool.name
}

func (agentPool *AcsAksAgentPool) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", agentPool.Id(), MinSize(), MaxSize())
}


func (acsMgr *AcsManager) GetResource(string vmType) string  {
	resourceGroup := acsMgr.Config.ResourceGroup
	clusterName := acsMgr.Config.AcsClusterName
	location := acsMgr.Config.Location

	// TODO: Assert for value not ACS/AKS
	if serviceType == autoscalerazure.ProviderNameACS {
		return resourceGroup + "_" + clusterName + "_" + location
	}
	return "MC_" + resourceGroup + "_" + clusterName + "_" + location
}

func (acsMgr *AcsManager) ProviderIDFromID(name string) string {
	return "azure://" + name
}

func (acsMgr *AcsManager) GetNodes() ([]string, error) {
	nodeResourceGroup := acsMgr.GetResourceForService()
	vmList, err := acsMgr.VMClient.List(nodeResourceGroup)
	if err != nil {
		glog.Error("Error", err)
		return nil, err
	}
	nodeArray := make([]string, len(*vmList.Value))
	for i, node := range *vmList.Value {
		providerID := acsMgr.ProviderIDFromID(*node.ID)
		nodeArray[i] = providerID
	}
	//TODO: return error for empty list.
	return nodeArray, nil
}


func (agentPool *AcsAksAgentPool) Nodes() ([]string, error) {
	return GetNodes()
}

func (agentPool *AcsAksAgentPool) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (agentPool *AcsAksAgentPool) Exist() bool {
	return true
}

func (agentPool *AcsAksAgentPool) Create() error {
	return cloudprovider.ErrAlreadyExist
}

func (agentPool *AcsAksAgentPool) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (agentPool *AcsAksAgentPool) Autoprovisioned() bool {
	return false
}
