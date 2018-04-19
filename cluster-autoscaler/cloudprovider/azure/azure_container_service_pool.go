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
	"github.com/golang/glog"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/compute"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
)

// AcsAksContainerServiceAgentPool implements NodeGroup interface for agent pools deployes in ACS/AKS
type ContainerServiceAgentPool struct {
	azureRef
	manager *AzureManager

	minSize           int
	maxSize           int
	serviceType       string
	nodeResourceGroup string

	template   map[string]interface{}
	parameters map[string]interface{}

	mutex       sync.Mutex
	lastRefresh time.Time
	curSize     int64
}

func NewContainerServiceAgentPool(spec *dynamic.NodeGroupSpec, am *AzureManager) (*ContainerServiceAgentPool, error) {
	asg := &ContainerServiceAgentPool{
		azureRef: azureRef{
			Name: spec.Name,
		},
		minSize: spec.MinSize,
		maxSize: spec.MaxSize,
		manager: am,
	}
	asg.serviceType = am.config.VMType
	asg.nodeResourceGroup = asg.GetNodeResourceGroup(asg.serviceType)
	return asg, nil
}

func (agentPool *ContainerServiceAgentPool) GetPool(agentProfiles *[]compute.ContainerServiceAgentPoolProfile) (ret *compute.ContainerServiceAgentPoolProfile) {
	for _, value := range *agentProfiles {
		if *(value.Name) == (agentPool.azureRef.Name) {
			return &value
		}
	}
	return nil
}

func (agentPool *ContainerServiceAgentPool) GetNodeCount(agentProfiles *[]compute.ContainerServiceAgentPoolProfile) (count int, err error) {
	pool := agentPool.GetPool(agentProfiles)
	if pool != nil {
		return (int)(*pool.Count), nil
	} else {
		return -1, fmt.Errorf("Could not find pool with name: %s", agentPool.azureRef)
	}
}

func (agentPool *ContainerServiceAgentPool) SetNodeCount(agentProfiles *[]compute.ContainerServiceAgentPoolProfile, count int) (err error) {
	pool := agentPool.GetPool(agentProfiles)
	if pool != nil {
		*(pool.Count) = int32(count)
		return nil
	} else {
		return fmt.Errorf("Could not find pool with name: %s", agentPool.azureRef)
	}
}

func (agentPool *ContainerServiceAgentPool) GetNodeResourceGroup(vmType string) string {
	resourceGroup := agentPool.manager.config.ResourceGroup
	clusterName := agentPool.manager.config.ClusterName
	location := agentPool.manager.config.Location

	// TODO: Assert for value not ACS/AKS
	if vmType == vmTypeACS {
		return resourceGroup + "_" + clusterName + "_" + location
	}
	return "MC_" + resourceGroup + "_" + clusterName + "_" + location
}

func (agentPool *ContainerServiceAgentPool) GetProviderID(name string) string {
	//TODO: come with a generic way to make it work with provider id formats
	// in different version of k8s.
	return "azure://" + name
}

func (agentPool *ContainerServiceAgentPool) GetNameFromProviderID(providerID string) (string, error) {
	vms, err := agentPool.manager.azClient.virtualMachinesClient.List(agentPool.nodeResourceGroup)
	if err != nil {
		return "", err
	}
	for _, vm := range *vms.Value {
		if strings.Compare(*vm.ID, providerID) == 0 {
			return *vm.Name, nil
		}
	}
	return "", fmt.Errorf("VM list empty")
}

//Public interfaces

func (agentPool *ContainerServiceAgentPool) MaxSize() int {
	return agentPool.maxSize
}

func (agentPool *ContainerServiceAgentPool) MinSize() int {
	return agentPool.minSize
}

// Undelying size of nodes
func (agentPool *ContainerServiceAgentPool) TargetSize() (int, error) {
	var err error
	if agentPool.serviceType == vmTypeACS {
		gotContainerService, err := agentPool.manager.azClient.containerServicesClient.Get(agentPool.manager.config.ResourceGroup,
			agentPool.manager.config.ClusterName)
	} else { // AKS
		gotContainerService, err := agentPool.manager.azClient.managedContainerServicesClient.Get(agentPool.manager.config.ResourceGroup,
			agentPool.manager.config.ClusterName)

	}
	if err != nil {
		glog.Error(err)
		return -1, err
	}
	return agentPool.GetNodeCount(gotContainerService.AgentPoolProfiles)
}

func (agentPool *ContainerServiceAgentPool) SetSize(targetSize int) error {
	glog.Infof("Set size request: %d", targetSize)
	if targetSize > agentPool.MaxSize() || targetSize < agentPool.MinSize() {
		glog.Errorf("Target size %d requested outside Max: %d, Min: %d", targetSize, agentPool.MaxSize(), agentPool.MaxSize)
		return fmt.Errorf("Target size %d requested outside Max: %d, Min: %d", targetSize, agentPool.MaxSize(), agentPool.MinSize())
	}

	if agentPool.servicetype == vmTypeAKS {
		gotContainerService, err := agentPool.manager.azClient.managedcontainerServicesClient.Get(agentPool.manager.config.ResourceGroup,
			agentPool.manager.config.ClusterName)
	} else {
		gotContainerService, err := agentPool.manager.azClient.containerServicesClient.Get(agentPool.manager.config.ResourceGroup,
			agentPool.manager.config.ClusterName)
	}
	if err != nil {
		glog.Error(err)
		return err
	}
	currentSize, err := agentPool.GetNodeCount(gotContainerService.AgentPoolProfiles)
	if err != nil {
		glog.Error(err)
		return err
	}

	glog.Infof("Current size: %d, Target size requested: %d", currentSize, targetSize)

	// Set the value in the volatile structure.
	agentPool.SetNodeCount(gotContainerService.AgentPoolProfiles, targetSize)

	cancel := make(chan struct{})

	var sp compute.ContainerServiceServicePrincipalProfile
	sp.ClientID = &(agentPool.manager.config.AADClientID)
	sp.Secret = &(agentPool.manager.config.AADClientSecret)
	gotContainerService.ServicePrincipalProfile = &sp

	//Update the service with the new value.
	if agentPool.ServiceType == vmTypeAKS {
		updatedVal, chanErr := agentPool.manager.azClient.managedContainerServicesClient.CreateOrUpdate(agentPool.manager.config.ResourceGroup, *gotContainerService.Name, gotContainerService, cancel)
	} else {
		updatedVal, chanErr := agentPool.manager.azClient.managedContainerServicesClient.CreateOrUpdate(agentPool.manager.config.ResourceGroup, *gotContainerService.Name, gotContainerService, cancel)
	}
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

func (agentPool *ContainerServiceAgentPool) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("Size increase must be +ve")
	}
	currentSize, err := agentPool.TargetSize()
	if err != nil {
		return err
	}
	targetSize := int(currentSize) + delta
	if targetSize > agentPool.MaxSize() {
		return fmt.Errorf("Size increase request of %d more than max size %d set", targetSize, agentPool.MaxSize())
	}
	return agentPool.SetSize(targetSize)
}

func (agentPool *ContainerServiceAgentPool) DeleteNodesInternal(providerIDs []string) error {
	cancel := new(<-chan struct{})
	currentSize, err := agentPool.TargetSize()
	if err != nil {
		return err
	}
	// Set the size to current size. Reduce as we are able to go through.
	targetSize := currentSize

	for _, providerID := range providerIDs {
		glog.Infof("ProviderID got to delete: %s", providerID)
		// Remove the "azure://" string from it
		vmID := strings.TrimPrefix(providerID, "azure://")
		nodeName, err := agentPool.GetNameFromProviderID(vmID)
		if err != nil {
			return err
		}
		glog.Infof("VM name got to delete: %s", nodeName)
		status, errChan := agentPool.manager.azClient.virtualMachinesClient.Delete(agentPool.nodeResourceGroup, nodeName, *cancel)
		if <-errChan != nil {
			return <-errChan
		}
		// TODO: Look more deeper into the operational status.
		glog.Infof("Status from delete: %+v", <-status)
		/*vhd := vm.StorageProfile.OsDisk.Vhd
		        managedDisk := vm.StorageProfile.OsDisk.ManagedDisk
				if vhd != nil {
					glog.Infof("vhd: %+v", vhd)
					accountName, vhdContainer, vhdBlob, err := armhelpers.SplitBlobURI(*vhd.URI)
				} else {
					glog.Infof("mgmtdisk: %+v", managedDisk)
				}*/
		targetSize--
	}

	// TODO: handle the errors from delete operation.
	if currentSize != targetSize {
		agentPool.SetSize(targetSize)
	}
	return nil
}

func (agentPool *ContainerServiceAgentPool) DeleteNodes(nodes []*apiv1.Node) error {
	var providerIDs []string
	for _, node := range nodes {
		glog.Infof("Node: %v", node)
		glog.Infof("Node: %s", node.Spec.ProviderID)
		providerIDs = append(providerIDs, node.Spec.ProviderID)
	}
	for _, p := range providerIDs {
		glog.Infof("ProviderID before calling acsmgr: %s", p)
	}
	return agentPool.DeleteNodesInternal(providerIDs)
}

func (agentPool *ContainerServiceAgentPool) GetNodes() ([]string, error) {
	vmList, err := agentPool.manager.azClient.virtualMachinesClient.List(agentPool.nodeResourceGroup)
	if err != nil {
		glog.Error("Error", err)
		return nil, err
	}
	nodeArray := make([]string, len(*vmList.Value))
	// TODO: figure out if we need to filter for only worker nodes.
	for i, node := range *vmList.Value {
		providerID := agentPool.GetProviderID(*node.ID)
		nodeArray[i] = providerID
	}
	//TODO: return error for empty list.
	return nodeArray, nil
}

func (agentPool *ContainerServiceAgentPool) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		return fmt.Errorf("Size decrease must be negative")
	}
	currentSize, err := agentPool.TargetSize()
	if err != nil {
		return err
	}
	// Get the current nodes in the list
	nodes, err := agentPool.GetNodes()
	if err != nil {
		return err
	}
	targetSize := int(currentSize) + delta
	if targetSize < len(nodes) {
		return fmt.Errorf("Attempt to decrease the size to:%d by reducing by: %d, which is below existing number of nodes: %d",
			targetSize, delta, len(nodes))
	}
	return agentPool.SetSize(targetSize)
}

func (agentPool *ContainerServiceAgentPool) Id() string {
	return agentPool.azureRef.Name
}

func (agentPool *ContainerServiceAgentPool) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", agentPool.Id(), agentPool.MinSize(), agentPool.MaxSize())
}

func (agentPool *ContainerServiceAgentPool) Nodes() ([]string, error) {
	return agentPool.GetNodes()
}

func (agentPool *ContainerServiceAgentPool) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (agentPool *ContainerServiceAgentPool) Exist() bool {
	return true
}

func (agentPool *ContainerServiceAgentPool) Create() error {
	return cloudprovider.ErrAlreadyExist
}

func (agentPool *ContainerServiceAgentPool) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (agentPool *ContainerServiceAgentPool) Autoprovisioned() bool {
	return false
}
