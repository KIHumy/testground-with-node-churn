package main

import (
	"context"
	"net"
	"os/exec"
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

type clientControl struct { //written by maintainer (Thorwin Bergholz)
	messageChannel              chan string //channel for the client reader (informational)
	churnChannel                chan string //channel to enable immediate reaktion to churn signal
	activeOrPassive             bool        //flag to check instance down or up
	connectionToController      net.Conn
	environment                 *runtime.RunEnv
	clientBootstrapInformation  string
	clientSynchronisation       sync.RWMutex
	clientChurnSynchronisation  sync.RWMutex
	networkBootstrapInformation []string //array of all network bootstrappers may not be up to date at all times!
	clientIPFSCids              []string
	netCids                     []string
	testEndFlag                 bool
	currentlyActiveRoutines     int
	churnMode                   string
	nodeAPI                     icore.CoreAPI
	clientIPFSNode              *core.IpfsNode
	motherContext               context.Context //mother Context used for deriving the context wich is usually context.WithCancel()
	context                     context.Context //context to synchronize all functions of netwrok behaviour during down
	contextCancelFunc           context.CancelFunc
	ipfsPortCounter             int
	ipfsRepoPath                string
	rolePrefix                  string
	numberOfIPFSRecords         int
	myRole                      string
}

func initiateNewClientControl(connection net.Conn, myRole string, environment *runtime.RunEnv) *clientControl { //written by maintainer (Thorwin Bergholz)
	return &clientControl{
		messageChannel:              make(chan string),
		churnChannel:                make(chan string),
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
	}
}

func (clientControl *clientControl) clientChurnReader() { //written by maintainer (Thorwin Bergholz)
	clientReadBuff := make([]byte, 2048)
	for {
		clientControl.connectionToController.SetReadDeadline(time.Now().Add(30 * time.Second))
		numberOfMessageBytes, error := clientControl.connectionToController.Read(clientReadBuff)
		clientControl.clientSynchronisation.RLock()
		if error != nil && clientControl.testEndFlag == false {
			clientControl.clientSynchronisation.RUnlock()
			clientControl.environment.RecordMessage("Reading of the client failed with: %v", error)
			continue
		} else if clientControl.testEndFlag {
			clientControl.clientSynchronisation.RUnlock()
			clientControl.connectionToController.Close()
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

func (clientControl *clientControl) returnBootstrapInformation() []string { //written by maintainer (Thorwin Bergholz)
	var sliceOfBootstrapInformation []string
	unProcessedMessage := <-clientControl.messageChannel                             //take bootstrapInformation from the messageChannel
	sliceOfBootstrapInformationWithPrefix := strings.Split(unProcessedMessage, "--") //split along seperator
	for index, information := range sliceOfBootstrapInformationWithPrefix {          //delete Prefix
		if index == 0 {
			continue
		} else {
			sliceOfBootstrapInformation = append(sliceOfBootstrapInformation, information)
		}
	}
	return sliceOfBootstrapInformation //return information as slice of strings
}

func (clientControl *clientControl) setNetworkBootstrapInformation() {
	var bootstrapSlice []string
	networkBootstrapString := <-clientControl.messageChannel
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

func (clientControl *clientControl) setNetCids() {
	var cidSlice []string
	networkBootstrapString := <-clientControl.messageChannel
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

// todo instruction handler is reached but unresponsive
func (clientControl *clientControl) startInstructionHandler(instruction string) {

	clientControl.environment.RecordMessage("Instruction handler acessed!!!!!!!!")
	if strings.Contains(instruction, "!!--send the bootstrap information.") { //response if bootstrap information for ipfs node is requested from this node
		clientControl.environment.RecordMessage("I could reach at least this response.")
		clientControl.clientSynchronisation.RLock()
		if clientControl.clientBootstrapInformation == "" {
			clientControl.sleepAndJitter()
			clientControl.clientSynchronisation.RUnlock()
			_, err := clientControl.connectionToController.Write([]byte("!!--Bootstrap information unavailabel at the moment.")) //tell node information to churn controller
			if err != nil {
				clientControl.environment.RecordMessage("Write to controller failed with: %v", err)
			} else {
				clientControl.environment.RecordMessage("I informed the controller that bootstrap information is unavailable at the moment.")
			}
		} else { //only if clause is reached else some how not i don't know why
			clientControl.sleepAndJitter()
			clientControl.environment.RecordMessage("Do you ever enter the ipfs response client.")
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
	if strings.Contains(instruction, "!!--Currently no recorded Cids.") {
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

func (clientControl *clientControl) requestNetworkBootstrapInfo() { //request the network bootstrap info
	clientControl.connectionToController.Write([]byte("!!--Please provide me the Bootstrap information."))
	clientControl.environment.RecordMessage("I requested the bootstrap information.")
}

func (clientControl *clientControl) setClientControlBootstrapInformation(bootstrapInfo string) {
	clientControl.clientSynchronisation.Lock()
	clientControl.clientBootstrapInformation = bootstrapInfo
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) sleepAndJitter() { //done to stop bursting and dosing the server
	jitter := rand.Intn(10) + 15
	sleepDuration := strconv.Itoa(jitter) + "s"
	sleepTime, error := time.ParseDuration(sleepDuration)
	if error != nil {
		clientControl.environment.RecordMessage("Parsing of duration failed with: %v", error)
	}

	time.Sleep(sleepTime)
}

func (clientControl *clientControl) returnTheNetworkBootstrappers() []string {
	return clientControl.networkBootstrapInformation
}

func (clientControl *clientControl) returnNetCids() []string {
	return clientControl.netCids
}

func (clientControl *clientControl) gossipIPFSCids() {
	messageString := "??IPFSNetworkCids"
	for _, cidStrings := range clientControl.clientIPFSCids {
		messageString = messageString + "--" + cidStrings
	}
	clientControl.connectionToController.Write([]byte(messageString))
}

func (clientControl *clientControl) setClientCids(cidSlice []string) {
	for _, cid := range cidSlice {
		if slices.Contains(clientControl.clientIPFSCids, cid) {
			continue
		} else {
			clientControl.clientIPFSCids = append(clientControl.clientIPFSCids, cid)
		}
	}
}

func (clientControl *clientControl) requestNetCids() {
	clientControl.sleepAndJitter()
	clientControl.connectionToController.Write([]byte("!!--Please send me the availabel CIDs in the Network."))
}

func (clientControl *clientControl) returnInstanceCids() []string {
	return clientControl.clientIPFSCids
}

func (clientControl *clientControl) announcePlanFinished() {
	clientControl.connectionToController.Write([]byte("!!--Finished plan."))
}

func (clientControl *clientControl) awaitCloseSignalAndCloseClientControl() {
	var closeSignal string
	closeSignal = ""
	for closeSignal != "??--Test ended you can close now." {
		closeSignal = <-clientControl.messageChannel
	}
	clientControl.clientSynchronisation.Lock()
	clientControl.testEndFlag = true
	clientControl.clientSynchronisation.Unlock()
	clientControl.churnChannel <- "Close yourself."
	clientControl.routineClientShutdownBarrier()
	close(clientControl.messageChannel)
	clientControl.environment.RecordMessage("Client instance shuts down.")
	return
}

func (clientControl *clientControl) increaseActiveRoutineCounter() {
	clientControl.clientSynchronisation.Lock()
	clientControl.currentlyActiveRoutines = clientControl.currentlyActiveRoutines + 1
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) decreaseActiveRoutineCounter() {
	clientControl.clientSynchronisation.Lock()
	clientControl.currentlyActiveRoutines = clientControl.currentlyActiveRoutines - 1
	clientControl.clientSynchronisation.Unlock()
}

func (clientControl *clientControl) routineClientShutdownBarrier() {
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

func (clientControl *clientControl) reportInstanceReadyForChurn() {
	clientControl.connectionToController.Write([]byte("!!--ready for churn."))
}

func (clientControl *clientControl) barrierBeforeChurn() {
	for {
		message := <-clientControl.messageChannel
		if message == "??--Controller is ready Churn starts." {
			return
		}
	}
}

func (clientControl *clientControl) setMotherContext(ctx context.Context) {
	clientControl.motherContext = ctx
}

func (clientControl *clientControl) fullBlock() {
	err := exec.Command("iptables", "-I", "OUTPUT", "1", "-d", "192.18.0.0/16", "-j", "ACCEPT").Run()
	if err != nil {
		clientControl.environment.RecordMessage("Firewall failed with: %v", err)
	}
	err = exec.Command("iptables", "-I", "INPUT", "1", "-s", "192.18.0.0/16", "-j", "ACCEPT").Run()
	if err != nil {
		clientControl.environment.RecordMessage("Firewall failed with: %v", err)
	}
	err = exec.Command("iptables", "-I", "OUTPUT", "1", "-d", clientControl.connectionToController.RemoteAddr().String(), "-j", "ACCEPT").Run()
	if err != nil {
		clientControl.environment.RecordMessage("Firewall failed with: %v", err)
	}
	err = exec.Command("iptables", "-I", "INPUT", "1", "-s", clientControl.connectionToController.RemoteAddr().String(), "-j", "ACCEPT").Run()
	if err != nil {
		clientControl.environment.RecordMessage("Firewall failed with: %v", err)
	}
	err = exec.Command("iptables", "-I", "OUTPUT", "3", "-j", "DROP").Run()
	if err != nil {
		clientControl.environment.RecordMessage("Firewall failed with: %v", err)
	}
	err = exec.Command("iptables", "-I", "INPUT", "3", "-j", "DROP").Run()
	if err != nil {
		clientControl.environment.RecordMessage("Firewall failed with: %v", err)
	}
}

func (clientControl *clientControl) fullUnblock() {
	for counterA := 0; counterA < 3; counterA++ {
		exec.Command("iptables", "-D", "INPUT", "1").Run()
	}
	for counterB := 0; counterB < 3; counterB++ {
		exec.Command("iptables", "-D", "OUTPUT", "1").Run()
	}
}

func sanitizeChurnMode(churnMode string) string {
	if churnMode != "down" && churnMode != "downAndRecover" {
		churnMode = "downAndRecover"
	}
	return churnMode
}

func (clientControl *clientControl) clientSideMessageSlicer(message string) {
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
			clientControl.messageChannel <- string(oneMessage)
		}
	}
	return
}
