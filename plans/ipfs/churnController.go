package main

import (
	"io"
	"net"
	goruntime "runtime"
	"strings"

	"slices"
	"sync"
	"time"

	"github.com/testground/sdk-go/runtime"
)

type basicNodeInfo struct { //the way node informations are stored at the controller (written by maintainer Thorwin Bergholz)
	nodeAdress          net.Addr
	role                string
	churnable           bool
	probabilityUp       float64
	probabilityDown     float64
	status              string
	connectionToTheNode net.Conn
}

type controlServer struct {
	controlAddr                  net.Addr
	listener                     net.Listener
	environment                  *runtime.RunEnv
	nodeInfoChannel              chan basicNodeInfo
	establishedConnections       chan net.Conn
	bootstrapInfo                []string
	globalNodeTable              []basicNodeInfo
	activeConnections            []net.Conn
	numberOfNetworkBootstrappers int
	isBootstrapPhase             bool
	readWriteSynchronisation     sync.RWMutex
	churnFrequency               frequency
	distribution                 string
	IPFSCids                     []string
	currentlyActiveInstances     int
	testEndFlag                  bool
	currentlyActiveRoutines      int
	churnMode                    string
	readyForChurnCounter         int
	availability                 float64
}

func startupChurnController(controlAddr net.Addr, numberOfInitialBootstrappers int, environment *runtime.RunEnv) {

	environment.RecordMessage("starting churn controller...")
	controlServer := initiateNewControler(controlAddr, numberOfInitialBootstrappers, environment)
	controlServer.increaseActiveRoutineCounter()
	go controlServer.bootstrapAssistance()
	controlServer.increaseActiveRoutineCounter()
	go controlServer.churnControlThread()
	err := controlServer.startController()
	if err != nil {
		environment.RecordMessage("controller failed with %v", err)
	}
	environment.RecordMessage("The adress was: %v", controlServer.controlAddr.String())
	environment.RecordMessage("The network was: %v", controlServer.controlAddr.Network())
}

func initiateNewControler(controlAddr net.Addr, numberOfInitialBootstrappers int, environment *runtime.RunEnv) *controlServer {

	return &controlServer{
		controlAddr:                  controlAddr,
		environment:                  environment,
		nodeInfoChannel:              make(chan basicNodeInfo),
		establishedConnections:       make(chan net.Conn, 100), //do increase this if the instances go beyond 30 also if you have time fix route cause that this blocks programm if to small
		bootstrapInfo:                []string{},
		globalNodeTable:              []basicNodeInfo{},
		activeConnections:            []net.Conn{},
		numberOfNetworkBootstrappers: numberOfInitialBootstrappers,
		isBootstrapPhase:             true,
		IPFSCids:                     []string{},
		currentlyActiveInstances:     environment.TestInstanceCount - 1, //all active instances in the network - controller
		testEndFlag:                  false,
		currentlyActiveRoutines:      0,
		readyForChurnCounter:         0,
	}
}

func (controlServer *controlServer) startController() error {
	defer close(controlServer.nodeInfoChannel)
	defer close(controlServer.establishedConnections)
	controlListener, err := net.Listen("tcp4", strings.Split(controlServer.controlAddr.String(), "/")[0]+":4500")
	controlServer.environment.RecordMessage("The listeners adress is: %v", controlListener.Addr())
	if err != nil {
		return err
	}

	defer controlListener.Close()
	controlServer.listener = controlListener

	var waitGroup sync.WaitGroup
	controlServer.increaseActiveRoutineCounter()
	waitGroup.Add(1)
	go controlServer.acceptanceRoutine(&waitGroup)

	waitGroup.Wait()

	return nil
}

func (controlServer *controlServer) acceptanceRoutine(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for {
		connection, err := controlServer.listener.Accept()
		controlServer.readWriteSynchronisation.RLock()
		if err != nil && controlServer.testEndFlag == false {
			controlServer.readWriteSynchronisation.RUnlock()
			controlServer.environment.RecordMessage("Connection failed, but continue! Error is: %v", err)
			continue
		} else if controlServer.testEndFlag {
			controlServer.readWriteSynchronisation.RUnlock()
			controlServer.environment.RecordMessage("Controller acepptance routine shuts down.")
			controlServer.decreaseActiveRoutineCounter()
			return
		} else {
			controlServer.readWriteSynchronisation.RUnlock()
		}
		controlServer.environment.RecordMessage("I established a connection!")
		controlServer.establishedConnections <- connection
		controlServer.increaseActiveRoutineCounter()
		go controlServer.readingRoutine(connection)
	}
}

func (controlServer *controlServer) readingRoutine(connection net.Conn) {

	defer connection.Close()
	readBuff := make([]byte, 2048)

	for {
		connection.SetReadDeadline(time.Now().Add(30 * time.Second))
		numberOfMessageBytes, err := connection.Read(readBuff)
		controlServer.readWriteSynchronisation.RLock()
		if err != nil && err != io.EOF && controlServer.testEndFlag == false {
			controlServer.readWriteSynchronisation.RUnlock()
			controlServer.environment.RecordMessage("Reading failed with %v", err)
			continue
		} else if controlServer.testEndFlag {
			controlServer.readWriteSynchronisation.RUnlock()
			connection.Close()
			controlServer.environment.RecordMessage("One reading routine of controller shuts down.")
			controlServer.decreaseActiveRoutineCounter()
			return
		} else {
			controlServer.readWriteSynchronisation.RUnlock()
		}
		message := readBuff[:numberOfMessageBytes]

		go controlServer.controlServerMessageSlicer(string(message), connection)

	}
}

func (controlServer *controlServer) bootstrapAssistance() { //written by maintainer (Thorwin Bergholz)

	var nodeTable []basicNodeInfo //table consisting of all the network information (so all instances execpt controller)
	controlServer.environment.RecordMessage("Churn Controller entered churn control.")
	for len(nodeTable) < controlServer.environment.TestInstanceCount-1 { //do not proceed until node map has been build. Subtract one because churn controller is counted as instance.

		newNodeInfo := <-controlServer.nodeInfoChannel
		controlServer.environment.RecordMessage("The length of the node table is %v.", len(nodeTable))
		controlServer.environment.RecordMessage("The length of the nodeInfoChannel is %v.", len(controlServer.nodeInfoChannel))

		if slices.Contains(nodeTable, newNodeInfo) { //do not insert nodes which occur multiple times again
			continue
		} else {
			nodeTable = append(nodeTable, newNodeInfo) //append new node to network overview
		}

	}
	controlServer.globalNodeTable = nodeTable
	controlServer.environment.RecordMessage("Initialization of node Table Sucessfull!")
	//var churnTable []basicNodeInfo //table of all nodes which participate in the node churn so only ones with churnable = true
	//for _, newChurningNode := range nodeTable {
	//	if newChurningNode.churnable {
	//		churnTable = append(churnTable, newChurningNode)
	//	} else {
	//		continue
	//	}
	//}

	//var bootStrappersInfoMessage string //generate message with all the information necessary for the bootstrappers to work
	//bootStrappersInfoMessage = "??"
	//for _, node := range nodeTable {
	//	if node.role == "bootstrapper" {
	//		bootStrappersInfoMessage = bootStrappersInfoMessage + "--" + node.nodeAdress.String()
	//	}
	//}
	//controlServer.environment.RecordMessage("Created Bootstrappers Info message: %v", bootStrappersInfoMessage)
	var activeConnectionsWithoutDuplicate []net.Conn //generated to remove duplicates and only remember active connections
	for len(activeConnectionsWithoutDuplicate) < controlServer.environment.TestInstanceCount-1 {
		connectionToNode := <-controlServer.establishedConnections
		if slices.Contains(activeConnectionsWithoutDuplicate, connectionToNode) {
			continue
		} else {
			activeConnectionsWithoutDuplicate = append(activeConnectionsWithoutDuplicate, connectionToNode)
		}
	}
	controlServer.activeConnections = activeConnectionsWithoutDuplicate
	controlServer.environment.RecordMessage("Initialization of all tables was successful begin now sending step to provide bootstrapinformation to clients.")
	//for _, nextRecipient := range activeConnectionsWithoutDuplicate { //TODO replace with correct step when finished
	//	numberOfByte, err := nextRecipient.Write([]byte(bootStrappersInfoMessage))
	//	if err != nil {
	//		controlServer.environment.RecordMessage("Sending of the bootstrapp information to %v failed with: %v", nextRecipient.RemoteAddr().String(), err)
	//	} else {
	//		controlServer.environment.RecordMessage("Sending the %v bytes Bootstrap information was succesful!", numberOfByte)
	//	}
	//} //end of section to remove
	//for _, currentConnection := range controlServer.activeConnections {
	//	adress := currentConnection.RemoteAddr().String()
	//	for _, node := range controlServer.globalNodeTable {
	//		if node.role == "bootstrapper" && adress == node.nodeAdress.String() {
	//			controlServer.requestBootstrapInfo(currentConnection)
	//			controlServer.environment.RecordMessage("Requested the bootstrap inormation from a bootstrapper.")
	//
	//		}
	//	}
	//}

	for controlServer.isBootstrapPhase {
		time.Sleep(10 * time.Second)
		if len(controlServer.bootstrapInfo) == controlServer.numberOfNetworkBootstrappers {
			controlServer.isBootstrapPhase = false
		}
	}

	controlServer.environment.RecordMessage("ChurnControl ends before shutdown!")
	controlServer.decreaseActiveRoutineCounter()
}

func (controlServer *controlServer) writeToNodeInfoChannel(connection net.Conn, message string) { //function written by maintainer (Thorwin Bergholz)

	messageContent := strings.Split(message, "--")
	if len(messageContent) == 3 && strings.Contains(message, "??nodeInfo") { //this is 3 at the moment because  strings contain only ??nodeInfo, role and if the node is chunable or not if more is added adjust length

		nodeAdress := connection.RemoteAddr()
		roleOfNewNode := messageContent[1]
		var nodeChurnable bool
		if messageContent[2] == "true" { //convert send string back into bool
			nodeChurnable = true
		} else {
			nodeChurnable = false
		}
		newNodeInfo := basicNodeInfo{ //generate new node Info to push it to channel

			nodeAdress:          nodeAdress,
			role:                roleOfNewNode,
			churnable:           nodeChurnable,
			probabilityUp:       0, //This is a placeholder as long as the distribution is not set.
			probabilityDown:     0, //This is a placeholder as long as the distribution is not set.
			status:              "up",
			connectionToTheNode: connection,
		}
		controlServer.readWriteSynchronisation.Lock()
		controlServer.nodeInfoChannel <- newNodeInfo //push node Info to channel
		controlServer.readWriteSynchronisation.Unlock()
	} else if strings.Contains(message, "??bootstrapListInfo") { //the if statement identifies the identifier for a message holding the ipfs bootstrap info of a client
		for index, infoString := range messageContent {
			if index == 0 {
				continue
			} else {
				controlServer.readWriteSynchronisation.Lock()
				controlServer.bootstrapInfo = append(controlServer.bootstrapInfo, infoString)
				controlServer.readWriteSynchronisation.Unlock()
			}
		}
	} else if strings.Contains(message, "??IPFSNetworkCids") {
		for index, infoString := range messageContent {
			controlServer.readWriteSynchronisation.RLock()
			if index == 0 || slices.Contains(controlServer.IPFSCids, infoString) { //if prefix or duplicate don't append
				controlServer.readWriteSynchronisation.RUnlock()
				continue
			} else {
				controlServer.readWriteSynchronisation.RUnlock()
				controlServer.readWriteSynchronisation.Lock()
				controlServer.IPFSCids = append(controlServer.IPFSCids, infoString)
				controlServer.readWriteSynchronisation.Unlock()
			}
		}
	} else {
		controlServer.environment.RecordMessage("Invalid Message reached Channel processing for Node Info Channel!")
	}

}

func (controlServer *controlServer) requestBootstrapInfo(currentConnection net.Conn) {
	currentConnection.Write([]byte("!!--send the bootstrap information."))
}

func (controlServer *controlServer) instructionHandler(currentConnection net.Conn, instruction string) { //handles instructions from clients
	if strings.Contains(instruction, "!!--Please provide me the Bootstrap information.") {
		controlServer.readWriteSynchronisation.RLock()                                     //lock for writers
		if len(controlServer.bootstrapInfo) < controlServer.numberOfNetworkBootstrappers { //if the number of bootstrappers is not set its zero and no records have been saved yet
			controlServer.readWriteSynchronisation.RUnlock()
			currentConnection.Write([]byte("!!--bootstrapInfo yet unavailable."))
		} else {
			bootstrapInfoAsString := "??"
			for _, nextPart := range controlServer.bootstrapInfo {
				bootstrapInfoAsString = bootstrapInfoAsString + "--" + nextPart
			}
			controlServer.readWriteSynchronisation.RUnlock()
			currentConnection.Write([]byte(bootstrapInfoAsString))
			controlServer.environment.RecordMessage("Send following message to client: %v", bootstrapInfoAsString)
		}

	}
	if strings.Contains(instruction, "!!--Bootstrap information unavailabel at the moment.") {
		time.Sleep(1 * time.Second)
		controlServer.requestBootstrapInfo(currentConnection)
	}
	if strings.Contains(instruction, "!!--Please send me the availabel CIDs in the Network.") {
		controlServer.readWriteSynchronisation.RLock()
		if len(controlServer.IPFSCids) == 0 {
			controlServer.readWriteSynchronisation.RUnlock()
			currentConnection.Write([]byte("!!--Currently no recorded Cids."))
		} else {
			controlServer.readWriteSynchronisation.RUnlock()
			messageString := "??netIPFSCids"
			controlServer.readWriteSynchronisation.Lock()
			cidSlice := controlServer.IPFSCids
			controlServer.readWriteSynchronisation.Unlock()
			for _, cid := range cidSlice {
				messageString = messageString + "--" + cid
			}
			currentConnection.Write([]byte(messageString))
		}
	}
	if strings.Contains(instruction, "!!--Finished plan.") {
		controlServer.readWriteSynchronisation.Lock()
		controlServer.currentlyActiveInstances = controlServer.currentlyActiveInstances - 1
		controlServer.readWriteSynchronisation.Unlock()
		controlServer.readWriteSynchronisation.RLock()
		if controlServer.currentlyActiveInstances == 0 {
			connSlice := controlServer.activeConnections
			controlServer.readWriteSynchronisation.RUnlock()
			for _, conn := range connSlice {
				conn.Write([]byte("??--Test ended you can close now."))
			}
			time.Sleep(30 * time.Second) //sleep to make sure everyone got the message
			controlServer.environment.RecordMessage("The test ended the Churn controller will now shut down.")
			controlServer.readWriteSynchronisation.Lock()
			controlServer.testEndFlag = true //synchronisation flag to shut down all relevant functions of the controller
			controlServer.readWriteSynchronisation.Unlock()
			controlServer.listener.Close()
			controlServer.routineServerShutdownBarrier()
			time.Sleep(30 * time.Second)
			controlServer.environment.RecordMessage("Open Goroutines after shutdown: %v", goruntime.NumGoroutine())
		} else {
			controlServer.readWriteSynchronisation.RUnlock()
		}
	}
	if strings.Contains(instruction, "!!--ready for churn.") {
		controlServer.readWriteSynchronisation.Lock()
		controlServer.readyForChurnCounter = controlServer.readyForChurnCounter + 1
		controlServer.readWriteSynchronisation.Unlock()
	}
}

func (controlServer *controlServer) increaseActiveRoutineCounter() {
	controlServer.readWriteSynchronisation.Lock()
	controlServer.currentlyActiveRoutines = controlServer.currentlyActiveRoutines + 1
	controlServer.readWriteSynchronisation.Unlock()
}

func (controlServer *controlServer) decreaseActiveRoutineCounter() {
	controlServer.readWriteSynchronisation.Lock()
	controlServer.currentlyActiveRoutines = controlServer.currentlyActiveRoutines - 1
	controlServer.readWriteSynchronisation.Unlock()
}

func (controlServer *controlServer) routineServerShutdownBarrier() {
	for {
		controlServer.readWriteSynchronisation.RLock()
		if controlServer.currentlyActiveRoutines <= 0 {
			controlServer.readWriteSynchronisation.RUnlock()
			return
		} else {
			controlServer.readWriteSynchronisation.RUnlock()
			time.Sleep(10 * time.Second)
		}
	}
}

func (controlServer *controlServer) controlServerMessageSlicer(message string, connection net.Conn) {
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
		if string(message) != "" {
			controlServer.environment.RecordMessage(string(oneMessage))
		}
		if strings.Contains(string(message), "!!") { //check if message is status message markt by "!!" Prefix
			go controlServer.instructionHandler(connection, string(oneMessage)) //handle instructions send by client
		}
		if strings.Contains(string(message), "??") { //check if message is an information message markt by "??" Prefix
			go controlServer.writeToNodeInfoChannel(connection, string(oneMessage))
		}
	}
	return
}
