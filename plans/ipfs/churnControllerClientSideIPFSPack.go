package main

import (
	"context"

	"github.com/ipfs/kubo/core"
	icore "github.com/ipfs/kubo/core/coreiface"
)

func (clientControl *clientControl) incrementPortNumber() { //This increments the port number so that a recovering peer uses a different port to ensure that no errors occure.
	newIPFSCounter := clientControl.ipfsPortCounter + 1
	if newIPFSCounter > 4999 {
		newIPFSCounter = 4001
	}
	clientControl.ipfsPortCounter = newIPFSCounter
}

func (clientControl *clientControl) setCoreIPFSNode(node *core.IpfsNode) { //Sets the current IPFS node that is hosted at this client.
	clientControl.clientSynchronisation.Lock()
	clientControl.clientIPFSNode = node
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) setNumberOfIPFSRecords(numberOfIPFSRecords int) { //This function sets the number of records this node will provide for the IPFS network.
	clientControl.numberOfIPFSRecords = numberOfIPFSRecords
}

func (clientControl *clientControl) setIPFSAPI(nodeAPI icore.CoreAPI) { //This function sets the node API of the IPFS node currently hosted at this instance.
	clientControl.nodeAPI = nodeAPI
}

func (clientControl *clientControl) setCancelContext(ctx context.Context, cancelMainFunc context.CancelFunc) { //This function sets a context that can be cancelled. It is mostly used for the down and recover functions of IPFS.
	clientControl.clientSynchronisation.Lock()
	clientControl.context = ctx
	clientControl.contextCancelFunc = cancelMainFunc
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) setipfsRepoPath(repoPath string) { //Sets the repo path for the IPFS node hosted at this instance.
	clientControl.ipfsRepoPath = repoPath
}

func (clientControl *clientControl) setRolePrefix(rolePrefix string) { //Sets the prefix for clientControl that can be used to derive the roles.
	clientControl.rolePrefix = rolePrefix
}

func (clientControl *clientControl) updateMyBootstrapInformation() { //Sends the bootstrap information of this node to the churn controller.
	clientControl.connectionToController.Write([]byte("??bootstrapListInfo--" + clientControl.clientBootstrapInformation))
}

func (clientControl *clientControl) ipfsDown() { //This function implements the down of the hosted IPFS node.
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
		clientControl.environment.RecordMessage("DHT closure failed with: %v", err)
	}
	err = clientControl.clientIPFSNode.Close()
	if err != nil {
		clientControl.environment.RecordMessage("Peer closure failed with: %v", err)
	}
}

func (clientControl *clientControl) ipfsRecover() { //This function implements the recover of the hosted IPFS node.
	newCancelContext, cancelFunc := context.WithCancel(clientControl.motherContext)

	// The following code is copied from the IPFS Kubo project.
	// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
	// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
	// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
	// Changes: Some variable names where modified. The error handling was changed to make it compatible to Testground and the error message was altered.
	// There are more input parameters because a modified version of the spawnEphemeral function has been used.
	// --- Begin of copied section (IPFS Kubo project) ---
	nodeAPI, node, err := spawnEphemeral(newCancelContext, clientControl.environment, clientControl, clientControl.connectionToController.LocalAddr())
	if err != nil {
		clientControl.environment.RecordMessage("Spawning node failed with: %v", err)
	}
	// --- End of copied section ---

	clientControl.setCoreIPFSNode(node)
	clientControl.setIPFSAPI(nodeAPI)
	clientControl.setCancelContext(newCancelContext, cancelFunc)
	generateFilesForRepo(clientControl.environment, newCancelContext, nodeAPI, clientControl.rolePrefix, clientControl, clientControl.numberOfIPFSRecords)
	var result string
	result = "uninitialized"
	result = customIPFSNetworkBootstrap(clientControl.environment, clientControl, nodeAPI, newCancelContext)
	if result == "bootstrapFailed" {
		clientControl.environment.RecordMessage("Bootstrap failed bootstrappers may be offline.")
	} else if result == "" {
		clientControl.environment.RecordMessage("Bootstrap was successfull.")
	}
}
