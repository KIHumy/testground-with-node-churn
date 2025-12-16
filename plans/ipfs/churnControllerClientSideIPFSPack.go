package main

import (
	"context"

	"github.com/ipfs/kubo/core"
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

func (clientControl *clientControl) setipfsRepoPath(repoPath string) {
	clientControl.ipfsRepoPath = repoPath
}

func (clientControl *clientControl) setRolePrefix(rolePrefix string) {
	clientControl.rolePrefix = rolePrefix
}

func (clientControl *clientControl) updateMyBootstrapInformation() {
	clientControl.connectionToController.Write([]byte("??bootstrapListInfo--" + clientControl.clientBootstrapInformation))
}

func (clientControl *clientControl) ipfsDown() {
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

func (clientControl *clientControl) ipfsRecover() {
	newCancelContext, cancelFunc := context.WithCancel(clientControl.motherContext)

	// The following code is adapted from the IPFS Kubo project.
	// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
	// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
	// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
	// Changes: Some variable names where modified. The error handling was changed to make it compatible to Testground and the error message was altered.
	// There are more input parameters because a modified version of the spawnEphemeral function has been used.
	// --- Begin of adapted section (IPFS Kubo project) ---
	nodeAPI, node, err := spawnEphemeral(newCancelContext, clientControl.environment, clientControl, clientControl.connectionToController.LocalAddr())
	if err != nil {
		clientControl.environment.RecordMessage("Spawning node failed with: %v", err)
	}
	// --- End of adapted section ---

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
