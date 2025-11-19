package main

import (
	"context"

	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	icore "github.com/ipfs/kubo/core/coreiface"
)

func (clientControl *clientControl) incrementPortNumber() {
	newIPFSCounter := clientControl.ipfsPortCounter + 1
	if newIPFSCounter > 4999 {
		newIPFSCounter = 4001
	}
	clientControl.ipfsPortCounter = newIPFSCounter
}

func (clientControl *clientControl) setCoreIPFSNode(node *core.IpfsNode) {
	clientControl.clientSynchronisation.Lock()
	clientControl.clientIPFSNode = node
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) setNumberOfIPFSRecords(numberOfIPFSRecords int) {
	clientControl.numberOfIPFSRecords = numberOfIPFSRecords
}

func (clientControl *clientControl) setIPFSAPI(nodeAPI icore.CoreAPI) {
	clientControl.nodeAPI = nodeAPI
}

func (clientControl *clientControl) setCancelContext(ctx context.Context, cancelMainFunc context.CancelFunc) {
	clientControl.clientSynchronisation.Lock()
	clientControl.context = ctx
	clientControl.contextCancelFunc = cancelMainFunc
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) ipfsChurnHandler() {
	for {
		instruction := <-clientControl.churnChannel
		clientControl.environment.RecordMessage("I reached the churn handler.")
		if instruction == "!!--down" {
			//clientControl.fullBlock()
			clientControl.clientChurnSynchronisation.Lock()
			clientControl.contextCancelFunc()
			for _, connection := range clientControl.clientIPFSNode.PeerHost.Network().Conns() {
				connection.Close()
			}
			err := clientControl.clientIPFSNode.PeerHost.Close()
			if err != nil {
				clientControl.environment.RecordMessage("PeerHost closure failed with: %v", err)
			}
			err = clientControl.clientIPFSNode.DHT.Close()
			if err != nil {
				clientControl.environment.RecordMessage("PeerHost closure failed with: %v", err)
			}
			//err = clientControl.clientIPFSNode.Repo.Close()
			//if err != nil {
			//	clientControl.environment.RecordMessage("Failed toClose the repo with: %v", err)
			//}
			err = clientControl.clientIPFSNode.Close()
			if err != nil {
				clientControl.environment.RecordMessage("PeerHost closure failed with: %v", err)
			}
			clientControl.clientChurnSynchronisation.Unlock()
		}
		if instruction == "!!--recover" {
			clientControl.clientChurnSynchronisation.Lock() //lock the variables when setting the ipfs node and the context and cancel function of the network behaviour
			newCancelContext, cancelFunc := context.WithCancel(clientControl.motherContext)

			// The following code is adapted from the IPFS Kubo project.
			// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
			// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE)
			// Changes: Some variable names where modified. The error handling was changed to make it compatible to Testground and the error message was altered.
			// There are more input parameters because a modified version of the spawnEphemeral function has been used.
			// --- Begin of adapted section (IPFS Kubo project) ---
			nodeAPI, node, err := spawnEphemeral(newCancelContext, clientControl.environment, clientControl, clientControl.connectionToController.LocalAddr())
			if err != nil {
				clientControl.environment.RecordMessage("Spawning node failed with: %v", err)
			}
			// --- End of adapted section ---

			clientControl.fullUnblock()
			clientControl.setCoreIPFSNode(node)
			clientControl.setIPFSAPI(nodeAPI)
			clientControl.setCancelContext(newCancelContext, cancelFunc)
			generateFilesForRepo(clientControl.environment, newCancelContext, nodeAPI, clientControl.rolePrefix, clientControl, clientControl.numberOfIPFSRecords)
			var result string
			result = "uninitialized"
			//for result != "" {
			result = customIPFSNetworkBootstrap(clientControl.environment, clientControl, nodeAPI, newCancelContext)
			if result == "bootstrapFailed" {
				clientControl.environment.RecordMessage("Bootstrap failed bootstrappers may be offline.")
			}
			if clientControl.myRole == "bootstrapper" {
				clientControl.updateMyBootstrapInformation()
			}
			//}
			//clientControl.recoverFunctionIPFS()
			clientControl.clientChurnSynchronisation.Unlock() //unly proceed with network behaviour after startup process is finished
		}
		if clientControl.testEndFlag == true {
			clientControl.decreaseActiveRoutineCounter()
			return
		}
	}
}

func (clientControl *clientControl) recoverFunctionIPFS() {
	clientControl.clientChurnSynchronisation.Lock()
	newCancelContext, cancelFunc := context.WithCancel(clientControl.motherContext)

	var successfullyInitialized bool
	var node *core.IpfsNode
	var api icore.CoreAPI
	successfullyInitialized = false
	for successfullyInitialized != true {
		clientControl.environment.RecordMessage("Begin node construction!")

		// The following code was copied from the IPFS Kubo project and then modified.
		// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
		// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE)
		// Changes: The error handling was changed to make it compatible to Testground and the error message was altered.
		// There are more input parameters because a modified version of the createNode function has been used. Included more logging to make the process more understandable.
		// --- Begin of copied section (IPFS Kubo project) ---
		node, err := createNode(newCancelContext, clientControl.ipfsRepoPath, clientControl.environment) //renamed variable to make the code more readable
		if err != nil {
			clientControl.environment.RecordMessage("Node could not be created because: %v", err)
		}
		clientControl.environment.RecordMessage("Finished node construction.")
		api, err = coreapi.NewCoreAPI(node)

		// --- End of copied section ---

		if err != nil {
			clientControl.environment.RecordMessage("Couldn't create API to node because: %v", err)
			node.Close()
		} else {
			successfullyInitialized = true
		}
	}

	//clientControl.fullUnblock()
	clientControl.setCoreIPFSNode(node)
	clientControl.setIPFSAPI(api)
	clientControl.setCancelContext(newCancelContext, cancelFunc)
	customIPFSNetworkBootstrap(clientControl.environment, clientControl, api, newCancelContext)
	clientControl.clientChurnSynchronisation.Unlock()
	return
}

func (clientControl *clientControl) setipfsRepoPath(repoPath string) {
	clientControl.ipfsRepoPath = repoPath
}

func (clientControl *clientControl) setRolePrefix(rolePrefix string) {
	clientControl.rolePrefix = rolePrefix
}

func (clientControl *clientControl) updateMyBootstrapInformation() {
	clientControl.connectionToController.Write([]byte("??bootstrapListInfo--" + clientControl.clientBootstrapInformation))
}
