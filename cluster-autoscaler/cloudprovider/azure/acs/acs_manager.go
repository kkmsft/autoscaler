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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/compute"
	"github.com/Azure/azure-sdk-for-go/arm/containerservice"
	"github.com/Azure/go-autorest/autorest"	
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"

	"github.com/golang/glog"
	"gopkg.in/gcfg.v1"
)

const (
	AKS = "aks"
	ACS = "acs"
)

// AcsManagerAPIs holds the methods which operates on underlying cloud provider
type AcsManagerAPIs interface {
	MaxSize() int
	MinSize() int
	CreateAgentPool() ([]*AgentPool, error)
	GetSize() (int, error)
	GetNodes() ([]string, error)
	DeleteNodes() ([]string, error)
	DeleteNode(nodeName string) error
	IncreaseSize(targetSize int) error
	DecreaseSize(targetSize int) error
	GetResourceForService() (string, error)
}

// AcsManager bring together all internal operations.
type AcsManager struct {
	ServiceType     string
	NodeResource    string
	Config          Configuration
	VMClient        compute.VirtualMachinesClient
	ServicesClient  containerservice.ContainerServicesClient
	Shutdown        chan struct{}
	MinSizeInternal int
	MaxSizeInternal int
}

// Configuration holds the configuration parsed from the --cloud-config flag
type Configuration struct {
	Cloud             string `json:"cloud" yaml:"cloud"`
	TenantID          string `json:"tenantId" yaml:"tenantId"`
	SubscriptionID    string `json:"subscriptionId" yaml:"subscriptionId"`
	AADClientID       string `json:"aadClientId" yaml:"aadClientId"`
	AADClientSecret   string `json:"aadClientSecret" yaml:"aadClientSecret"`
	ResourceGroup     string `json:"resourceGroup" yaml:"resourceGroup"`
	NodeResourceGroup string `json:"nodeResourceGroup" yaml:"nodeResourceGroup"`
	AcsClusterName    string `json:"acsClusterName" yaml:"acsClusterName"`
	Location          string `json:"location" yaml:"location"`
}

/*
func withInspection() autorest.PrepareDecorator {
	return func(p autorest.Preparer) autorest.Preparer {
		return autorest.PreparerFunc(func(r *http.Request) (*http.Request, error) {
			glog.Infof("Inspecting Request: %s %s\n", r.Method, r.URL)
			return p.Prepare(r)
		})
	}
}

func byInspecting() autorest.RespondDecorator {
	return func(r autorest.Responder) autorest.Responder {
		return autorest.ResponderFunc(func(resp *http.Response) error {
			glog.Infof("Inspecting Response: %s for %s %s\n", resp.Status, resp.Request.Method, resp.Request.URL)
			return r.Respond(resp)
		})
	}
}
*/

// CreateAcsManager We will get either "acs" or "aks" in the serviceType
func CreateAcsManager(configReader io.Reader, serviceType string) (*AcsManager, error) {
    if configReader != nil {
	    var cfg Configuration
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			glog.Errorf("Couldn't read config: %v", err)
			return nil, err
		}

		/*subscriptionID := cfg.SubscriptionID
		resourceGroup := cfg.ResourceGroup
		clientSecret := cfg.AADClientSecret
		tenantId := cfg.TenantID
		clientId := cfg.AADClientID
		AcsClusterName := cfg.AcsClusterName
		location := cfg.Location*/
	} else {
		var cfg Configuration
		file, e := ioutil.ReadFile("/etc/acs/azure.json")
		if e != nil {
			glog.Errorf("Error: %v", e)
			return nil, e
		}
		unmarshalE := json.Unmarshal(file, &cfg)
		if unmarshalE != nil {
			glog.Errorf("Error: %v", unmarshalE)
			return nil, unmarshalE
		}
		glog.Infof("Output:%+v", cfg)

		subscriptionID := cfg.SubscriptionID
		clientSecret := cfg.AADClientSecret
		tenantID := cfg.TenantID
		clientID := cfg.AADClientID

		env, envErr := azure.EnvironmentFromName(cfg.Cloud)
		if envErr != nil {
			glog.Errorf("Error: %v", envErr)
			return nil, envErr
		}

		// TODO: Check the values validity
		// We first obtain the service principle token.
		spt, err := NewServicePrincipalTokenFromCredentials(tenantID, clientID, clientSecret, env.ServiceManagementEndpoint)
		if err != nil {
			return nil, err
		}

		// Use the service principle token to get the client.
		containerServicesClient := containerservice.NewContainerServicesClient(subscriptionID)
		containerServicesClient.Authorizer = autorest.NewBearerAuthorizer(spt)
		containerServicesClient.Sender = autorest.CreateSender()

		vmClient := compute.NewVirtualMachinesClient(subscriptionID)
		vmClient.Authorizer = autorest.NewBearerAuthorizer(spt)
		vmClient.Sender = autorest.CreateSender()

		acsManager := &AcsManager{
			ServiceType:     serviceType,
			Config:          cfg,
			ServicesClient:  containerServicesClient,
			VMClient:        vmClient,
			Shutdown:        make(chan struct{}),
			MaxSizeInternal: 10,
			MinSizeInternal: 2,
		}
		nodeResource := acsManager.GetResourceForService()
		if nodeResource == "" {
			return nil, fmt.Errorf("Error getting node resource")
		} else {
			acsManager.NodeResource = nodeResource
		}
		glog.Infof("Creating the acsmanager:  min: %d, max: %d", acsManager.MinSize(), acsManager.MaxSize())
		return acsManager, nil
	}
	return nil, fmt.Errorf("Could not create acsmanager!!")
}

func NewServicePrincipalTokenFromCredentials(tenantID string, clientID string, clientSecret string, scope string) (*adal.ServicePrincipalToken, error) {
	oauthConfig, err := adal.NewOAuthConfig(azure.PublicCloud.ActiveDirectoryEndpoint, tenantID)
	if err != nil {
		panic(err)
	}
	return adal.NewServicePrincipalToken(*oauthConfig, clientID, clientSecret, scope)
}

func (acsMgr *AcsManager) Cleanup() {
	// TODO: Fill up...
}

// CreateAgentPool
func (acsMgr *AcsManager) CreateAgentPool() ([]*AgentPool, error) {
	var agentPool = new(AgentPool)
	agentPool.acsMgr = acsMgr
	agentPool.name = acsMgr.Config.AcsClusterName
	agentArray := make([]*AgentPool, 1)
	agentArray[0] = agentPool
	return agentArray, nil
}

func (acsMgr *AcsManager) GetResourceForService() string {

	serviceType := acsMgr.ServiceType
	resourceGroup := acsMgr.Config.ResourceGroup
	clusterName := acsMgr.Config.AcsClusterName
	location := acsMgr.Config.Location

	// TODO: Assert for value not ACS/AKS
	if serviceType == ACS {
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

func (acsMgr *AcsManager) MaxSize() int {
	return acsMgr.MaxSizeInternal
}

func (acsMgr *AcsManager) MinSize() int {
	return acsMgr.MinSizeInternal
}

func (acsMgr *AcsManager) GetSize() (int, error) {
	gotContainerService, err := acsMgr.ServicesClient.Get(acsMgr.Config.ResourceGroup, acsMgr.Config.AcsClusterName)
	if err != nil {
		glog.Error(err)
		return -1, err
	}
	agentProfile := *(gotContainerService.AgentPoolProfiles)
	poolSize := int(*(agentProfile[0].Count))
	glog.Infof("Get size reporting size: %d", poolSize)
	return poolSize,nil
}

func (acsMgr *AcsManager) SetSize(targetSize int) error {
	glog.Infof("Set size request: %d", targetSize)
	if targetSize > acsMgr.MaxSize() || targetSize < acsMgr.MinSize() {
		glog.Errorf("Target size %d requested outside Max: %d, Min: %d", targetSize, acsMgr.MaxSize, acsMgr.MinSize)
		return fmt.Errorf("Target size %d requested outside Max: %d, Min: %d", targetSize, acsMgr.MaxSize, acsMgr.MinSize)
	}

	gotContainerService, err := acsMgr.ServicesClient.Get(acsMgr.Config.ResourceGroup, acsMgr.Config.AcsClusterName)
	if err != nil {
		glog.Error(err)
		return err
	}
	currentSize := int(*(*gotContainerService.AgentPoolProfiles)[0].Count)

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

func (acsMgr *AcsManager) GetNameFromProviderID(providerID string) (string, error) {
	vms, err := acsMgr.VMClient.List(acsMgr.Config.NodeResourceGroup)
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

func (acsMgr *AcsManager) DeleteNodes(providerIDs []string) error {
	nodeResourceGroup := acsMgr.GetResourceForService()
	cancel := new(<-chan struct{})	
	currentSize, err := acsMgr.GetSize()
	if err != nil {
		return err
	}
	// Set the size to current size. Reduce as we are able to go through.
	targetSize := currentSize

	for _, providerID := range providerIDs {
		glog.Infof("ProviderID got to delete: %s", providerID)
		// Remove the "azure://" string from it
		vmID := strings.TrimPrefix(providerID, "azure://")
		nodeName, err := acsMgr.GetNameFromProviderID(vmID)
		if err != nil {
			return err
		}
		glog.Infof("VM name got to delete: %s", nodeName)
		status, errChan := acsMgr.VMClient.Delete(nodeResourceGroup, nodeName, *cancel)
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
	if (currentSize != targetSize) {
		acsMgr.SetSize(targetSize)
	}	
	return nil
}
