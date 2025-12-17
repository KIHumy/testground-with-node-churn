package main

import (
	"context"
	"net"
	"slices"
	"strconv"
	"time"

	"strings"
	"sync"

	"math/rand"

	"github.com/ipfs/kubo/core"
	icore "github.com/ipfs/kubo/core/coreiface"
	"github.com/testground/sdk-go/runtime"
)

type clientControl struct { //the struct for the interface the client uses to communicate to the churn controller
	messageChannel              chan string //channel for the client reader (informational)
	churnChannel                chan string //channel to enable immediate reaction to churn
	shutdownChannel             chan string
	activeOrPassive             bool //flag to check instance down or up
	connectionToController      net.Conn
	environment                 *runtime.RunEnv
	clientBootstrapInformation  string
	clientSynchronisation       sync.RWMutex
	clientChurnSynchronisation  sync.RWMutex
	patienceSynchronisation     sync.RWMutex
	networkBootstrapInformation []string //array of all network bootstrappers may not be up to date at all times!
	clientIPFSCids              []string
	netCids                     []string
	testEndFlag                 bool
	currentlyActiveRoutines     int
	churnMode                   string
	nodeAPI                     icore.CoreAPI
	clientIPFSNode              *core.IpfsNode
	motherContext               context.Context //mother context used for deriving the context wich is usually context.WithCancel()
	context                     context.Context //context to synchronize all functions of network behaviour during down
	contextCancelFunc           context.CancelFunc
	cancelFrustrationTimer      context.CancelFunc
	ipfsPortCounter             int
	ipfsRepoPath                string
	rolePrefix                  string
	numberOfIPFSRecords         int
	myRole                      string
	patience                    time.Duration
	patienceEnabled             bool
	frustrationTimer            context.Context
	protocol                    string
}

func initiateNewClientControl(connection net.Conn, myRole string, instancePatience time.Duration, instancePatienceEnabled bool, protocol string, environment *runtime.RunEnv) *clientControl { //This function generates a new client control instance.
	return &clientControl{
		messageChannel:              make(chan string),
		churnChannel:                make(chan string),
		shutdownChannel:             make(chan string),
		activeOrPassive:             true,
		connectionToController:      connection,
		environment:                 environment,
		clientBootstrapInformation:  "",
		networkBootstrapInformation: []string{},
		clientIPFSCids:              []string{},
		netCids:                     []string{},
		testEndFlag:                 false,
		currentlyActiveRoutines:     0,
		churnMode:                   sanitizeChurnMode(environment.StringParam("churnMode")),
		ipfsPortCounter:             4001,
		myRole:                      myRole,
		patience:                    instancePatience,
		patienceEnabled:             instancePatienceEnabled,
		protocol:                    protocol,
	}
}

func (clientControl *clientControl) clientChurnReader() { //This is the reader of the client. It is used to receive messages from the control network.
	clientReadBuff := make([]byte, 2048)
	for {
		clientControl.connectionToController.SetReadDeadline(time.Now().Add(60 * time.Second))
		numberOfMessageBytes, error := clientControl.connectionToController.Read(clientReadBuff)
		clientControl.clientSynchronisation.RLock()
		if error != nil && clientControl.testEndFlag == false {
			clientControl.clientSynchronisation.RUnlock()
			clientControl.environment.RecordMessage("Reading of the client failed with: %v", error)
			continue
		} else if clientControl.testEndFlag {
			clientControl.clientSynchronisation.RUnlock()
			clientControl.environment.RecordMessage("Instance reader shuts down.")
			clientControl.decreaseActiveRoutineCounter()
			return
		} else {
			clientControl.clientSynchronisation.RUnlock()
		}
		message := clientReadBuff[:numberOfMessageBytes]
		go clientControl.clientSideMessageSlicer(string(message))
	}
}

func (clientControl *clientControl) setNetworkBootstrapInformation() { //This function is used to receive and set the network bootstrap information of the IPFS network.
	var bootstrapSlice []string
	var networkBootstrapString string
	gotTheCorrectMessage := false
	for gotTheCorrectMessage == false {
		networkBootstrapString = <-clientControl.messageChannel
		if strings.Contains(networkBootstrapString, "??bootstrapInfo") {
			gotTheCorrectMessage = true
		}
	}

	networkBootInformation := strings.Split(networkBootstrapString, "--")
	for index, information := range networkBootInformation {
		if index == 0 {
			continue
		} else {
			bootstrapSlice = append(bootstrapSlice, information)
		}
	}
	clientControl.networkBootstrapInformation = bootstrapSlice
}

func (clientControl *clientControl) setNetCids() { //This function is there to receive and set the CIDs available in the network.
	var cidSlice []string
	gotTheCorrectMessage := false
	var networkBootstrapString string
	for gotTheCorrectMessage == false {
		networkBootstrapString = <-clientControl.messageChannel
		if strings.Contains(networkBootstrapString, "??netIPFSCids") {
			gotTheCorrectMessage = true
		}
	}
	networkBootInformation := strings.Split(networkBootstrapString, "--")
	for index, information := range networkBootInformation {
		if index == 0 {
			continue
		} else {
			cidSlice = append(cidSlice, information)
		}
	}
	clientControl.netCids = cidSlice
}

func (clientControl *clientControl) startInstructionHandler(instruction string) { //This function is used to respond to requests of the churn controller.

	if strings.Contains(instruction, "!!--send the bootstrap information.") { //response if bootstrap information for ipfs node is requested from this node
		clientControl.clientSynchronisation.RLock()
		if clientControl.clientBootstrapInformation == "" {
			clientControl.sleepAndJitter()
			clientControl.clientSynchronisation.RUnlock()
			_, err := clientControl.connectionToController.Write([]byte("!!--Bootstrap information unavailable at the moment.")) //tell node information to churn controller
			if err != nil {
				clientControl.environment.RecordMessage("Write to controller failed with: %v", err)
			} else {
				clientControl.environment.RecordMessage("I informed the controller that bootstrap information is unavailable at the moment.")
			}
		} else {
			clientControl.sleepAndJitter()
			numberOfBytes, err := clientControl.connectionToController.Write([]byte("??bootstrapListInfo--" + clientControl.clientBootstrapInformation)) //tell node information to churn controller
			clientControl.clientSynchronisation.RUnlock()
			if err != nil {
				clientControl.environment.RecordMessage("Write to controller failed with: %v", err)
			} else {
				clientControl.environment.RecordMessage("I wrote %v number of ipfs bootstrap information to the controller.", numberOfBytes)
			}
		}

	}
	if strings.Contains(instruction, "!!--bootstrapInfo yet unavailable.") { //response if controller has the network bootstrap information not at hand
		clientControl.sleepAndJitter()
		clientControl.requestNetworkBootstrapInfo()
	}
	if strings.Contains(instruction, "!!--Currently no recorded CIDs.") {
		clientControl.sleepAndJitter()
		clientControl.requestNetCids()
	}
	if strings.Contains(instruction, "!!--down") {
		clientControl.environment.RecordMessage("Received down Signal!")
		clientControl.churnChannel <- instruction
	}
	if strings.Contains(instruction, "!!--recover") {
		clientControl.environment.RecordMessage("Received recover Signal!")
		clientControl.churnChannel <- instruction
	}
}

func (clientControl *clientControl) requestNetworkBootstrapInfo() { //Request the network bootstrap info.
	clientControl.connectionToController.Write([]byte("!!--Please provide me the Bootstrap information."))
	clientControl.environment.RecordMessage("I requested the bootstrap information.")
}

func (clientControl *clientControl) setClientControlBootstrapInformation(bootstrapInfo string) { //This function sets the information over which CIDs the client provides to others.
	clientControl.clientSynchronisation.Lock()
	clientControl.clientBootstrapInformation = bootstrapInfo
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) sleepAndJitter() { //Done to reduce the load on the server.
	jitter := rand.Intn(10) + 15
	sleepDuration := strconv.Itoa(jitter) + "s"
	sleepTime, error := time.ParseDuration(sleepDuration)
	if error != nil {
		clientControl.environment.RecordMessage("Parsing of duration failed with: %v", error)
	}

	time.Sleep(sleepTime)
}

func (clientControl *clientControl) returnTheNetworkBootstrappers() []string { //gives the network bootstrappers back
	return clientControl.networkBootstrapInformation
}

func (clientControl *clientControl) returnNetCids() []string { //gives the network CIDs back
	return clientControl.netCids
}

func (clientControl *clientControl) gossipIPFSCids() { //This sends the CIDs this node provides to the churn controler.
	messageString := "??IPFSNetworkCids"
	for _, cidStrings := range clientControl.clientIPFSCids {
		messageString = messageString + "--" + cidStrings
	}
	clientControl.connectionToController.Write([]byte(messageString))
}

func (clientControl *clientControl) setClientCids(cidSlice []string) { //This sets the CIDs the client has present.
	for _, cid := range cidSlice {
		if slices.Contains(clientControl.clientIPFSCids, cid) {
			continue
		} else {
			clientControl.clientIPFSCids = append(clientControl.clientIPFSCids, cid)
		}
	}
}

func (clientControl *clientControl) requestNetCids() { //This is used to request the CIDs of all nodes in the network.
	clientControl.sleepAndJitter()
	clientControl.connectionToController.Write([]byte("!!--Please send me the available CIDs in the Network."))
}

func (clientControl *clientControl) returnInstanceCids() []string { //This is used to return the CIDs of the client.
	return clientControl.clientIPFSCids
}

func (clientControl *clientControl) announcePlanFinished() { //This is used to announce that the instance has finished its tasks.
	clientControl.connectionToController.Write([]byte("!!--Finished plan."))
}

func (clientControl *clientControl) announceShutdownFinished() { //This is used to show the churn controller that the shutdown has finished.
	clientControl.connectionToController.Write([]byte("!!--Shutdown finished."))
}

func (clientControl *clientControl) awaitCloseSignalAndCloseClientControl() { //This function is there to shutdown this client control.
	var closeSignal string
	closeSignal = ""
	for closeSignal != "??--Test ended you can close now." {
		closeSignal = <-clientControl.shutdownChannel
	}
	clientControl.clientChurnSynchronisation.Lock()
	clientControl.clientSynchronisation.Lock()
	clientControl.testEndFlag = true
	clientControl.clientSynchronisation.Unlock()
	clientControl.churnChannel <- "Close yourself."
	clientControl.routineClientShutdownBarrier()
	close(clientControl.messageChannel)
	close(clientControl.shutdownChannel)
	clientControl.environment.RecordMessage("Client instance shuts down.")
	clientControl.clientChurnSynchronisation.Unlock()
	clientControl.announceShutdownFinished()
	clientControl.connectionToController.Close()
	return
}

func (clientControl *clientControl) increaseActiveRoutineCounter() { //Increases the tracker of how many routines of this client control are still active.
	clientControl.clientSynchronisation.Lock()
	clientControl.currentlyActiveRoutines = clientControl.currentlyActiveRoutines + 1
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) decreaseActiveRoutineCounter() { //Decreases the tracker of how many routines of this client control are still active.
	clientControl.clientSynchronisation.Lock()
	clientControl.currentlyActiveRoutines = clientControl.currentlyActiveRoutines - 1
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) routineClientShutdownBarrier() { //This barrier is there to enforce a shutdown of all client control routines.
	for {
		clientControl.clientSynchronisation.RLock()
		if clientControl.currentlyActiveRoutines <= 0 {
			clientControl.clientSynchronisation.RUnlock()
			return
		} else {
			clientControl.clientSynchronisation.RUnlock()
			time.Sleep(10 * time.Second)
		}
	}
}

func (clientControl *clientControl) reportInstanceReadyForChurn() { //This notifies the controller that the client is ready for the churn to start.
	clientControl.connectionToController.Write([]byte("!!--ready for churn."))
}

func (clientControl *clientControl) barrierBeforeChurn() { //This functions as barrier it can be used to stop the client until he gets the start signal from the controller.
	for {
		message := <-clientControl.messageChannel
		if message == "??--Controller is ready Churn starts." {
			return
		}
	}
}

func (clientControl *clientControl) setMotherContext(ctx context.Context) { //This function sets a context which can be used to derive contexts for the frustration functions and the cancel context for the node.
	clientControl.motherContext = ctx
}

func sanitizeChurnMode(churnMode string) string { //This function is there to stop invalid churn modes from accessing the test.
	if churnMode != "down" && churnMode != "downAndRecover" {
		churnMode = "downAndRecover"
	}
	return churnMode
}

func (clientControl *clientControl) clientSideMessageSlicer(message string) { //This function is there to seperate messages which are glued together.
	var messageSlice []string
	numberOfInstructionsInMessage := strings.Count(message, "!!")
	numberOfInformationsInMessage := strings.Count(message, "??")
	if (numberOfInstructionsInMessage + numberOfInformationsInMessage) > 1 {
		var precursorSlice []string
		sliceAtInstructions := strings.Split(message, "!!")
		for index, _ := range sliceAtInstructions {
			if sliceAtInstructions[index] == "" {
				continue
			} else if strings.HasPrefix(sliceAtInstructions[index], "??") {
				precursorSlice = append(precursorSlice, sliceAtInstructions[index])
			} else {
				precursorSlice = append(precursorSlice, "!!"+sliceAtInstructions[index])
			}
		}
		for _, messageWithoutInstructionGlue := range precursorSlice {
			seperatedSlice := strings.Split(messageWithoutInstructionGlue, "??")
			for _, seperatedMessage := range seperatedSlice {
				if seperatedMessage == "" {
					continue
				} else if strings.HasPrefix(seperatedMessage, "!!") {
					messageSlice = append(messageSlice, seperatedMessage)
				} else {
					messageSlice = append(messageSlice, "??"+seperatedMessage)
				}
			}
		}
	} else {
		messageSlice = append(messageSlice, message)
	}
	for _, oneMessage := range messageSlice {
		clientControl.environment.RecordMessage("I, a client, received this message: %v", string(oneMessage))
		if strings.Contains(string(oneMessage), "!!") { //instruction messages are marked by "!!"
			go clientControl.startInstructionHandler(string(oneMessage))
		}
		if strings.Contains(string(oneMessage), "??") { //information messages are marked by "??"
			if string(oneMessage) == "??--Test ended you can close now." {
				clientControl.shutdownChannel <- string(oneMessage)
			} else {
				clientControl.messageChannel <- string(oneMessage)
			}
		}
	}
	return
}

func (clientControl *clientControl) communicateNodeInfoToController(role string, churnable string) { //This function is there to tell the churn controller the clients role and whether he is susceptible to churn or not.
	numberOfBytes, err := clientControl.connectionToController.Write([]byte("??nodeInfo--" + role + "--" + churnable)) //tell node information to churn controller
	if err != nil {
		clientControl.environment.RecordMessage("Write to controller failed with: %v", err)
	}
	clientControl.environment.RecordMessage("Wrote %v bytes to controller", numberOfBytes)
}

func (clientControl *clientControl) startFrustrationTimer() { //Sets the timer and cancel function so that the node tracks how long it did not make progress.
	clientControl.patienceSynchronisation.Lock()
	clientControl.frustrationTimer, clientControl.cancelFrustrationTimer = context.WithTimeout(clientControl.motherContext, clientControl.patience)
	clientControl.patienceSynchronisation.Unlock()
}

func (clientControl *clientControl) resetFrustration() { //Resets the timer until the node will shutdown if not making progress.
	clientControl.patienceSynchronisation.Lock()
	clientControl.cancelFrustrationTimer()
	clientControl.frustrationTimer, clientControl.cancelFrustrationTimer = context.WithTimeout(clientControl.motherContext, clientControl.patience)
	clientControl.patienceSynchronisation.Unlock()
}

func (clientControl *clientControl) checkFrustration() bool { //Checks if the timer until a node should make progress has expired or not.
	if clientControl.patienceEnabled {
		clientControl.patienceSynchronisation.RLock()
		select {
		case <-clientControl.frustrationTimer.Done():
			clientControl.environment.RecordMessage("Instance didn't make progress and will now finish its plan regardless of the task frustrationTimeout expired.")
			clientControl.patienceSynchronisation.RUnlock()
			return true //instance aborts task due to lacking progress
		default:
			clientControl.patienceSynchronisation.RUnlock()
			return false //instance continues with their tasks
		}
	}
	return false //if patience is disabled node will always continue with its tasks
}

func (clientControl *clientControl) churnHandler() { //This function executes the reactions to churn signals from the churn controller based on the signal it received.
	for {
		instruction := <-clientControl.churnChannel
		if instruction == "!!--down" {
			clientControl.clientChurnSynchronisation.Lock()
			if clientControl.testEndFlag {
				clientControl.decreaseActiveRoutineCounter()
				clientControl.clientChurnSynchronisation.Unlock()
				return
			}
			if clientControl.protocol == "IPFS" {
				clientControl.ipfsDown()
			} else {
				clientControl.environment.RecordMessage("The entered protocol is not available, because of that the protocol will not show a reaction to down and recover.")
			}
			clientControl.clientChurnSynchronisation.Unlock()
		}
		if instruction == "!!--recover" {
			clientControl.clientChurnSynchronisation.Lock() //lock the variables when setting the ipfs node and the context and cancel function of the network behaviour
			if clientControl.testEndFlag {
				clientControl.decreaseActiveRoutineCounter()
				clientControl.clientChurnSynchronisation.Unlock()
				return
			}
			if clientControl.protocol == "IPFS" {
				clientControl.ipfsRecover()
			} else {
				clientControl.environment.RecordMessage("The entered protocol is not available, because of that the protocol will not show a reaction to down and recover.")
			}
			clientControl.clientChurnSynchronisation.Unlock() //only proceed with network behaviour after startup process is finished
		}
		if clientControl.testEndFlag == true {
			clientControl.decreaseActiveRoutineCounter()
			return
		}
	}
}
