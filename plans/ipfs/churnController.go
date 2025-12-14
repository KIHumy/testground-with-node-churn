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

type basicNodeInfo struct { //the way node informations are stored at the controller
	nodeAdress          net.Addr
	role                string
	churnable           bool
	probabilityUp       float64
	probabilityDown     float64
	status              string
	connectionToTheNode net.Conn
}

// the code for the basic server struct was inspired by the tutorial https://www.youtube.com/watch?v=qJQrrscB1-4
type controlServer struct {
	controlAddr                      net.Addr
	listener                         net.Listener
	environment                      *runtime.RunEnv
	nodeInfoChannel                  chan basicNodeInfo
	establishedConnections           chan net.Conn
	bootstrapInfo                    []string
	globalNodeTable                  []basicNodeInfo
	activeConnections                []net.Conn
	numberOfNetworkBootstrappers     int
	readWriteSynchronisation         sync.RWMutex
	churnEndFlagSynchronisation      sync.RWMutex
	barrierLock                      sync.RWMutex
	churnFrequency                   frequency
	distribution                     string
	IPFSCids                         []string
	currentlyActiveInstances         int
	testEndFlag                      bool
	churnEndFlag                     bool
	currentlyActiveRoutines          int
	churnMode                        string
	readyForChurnCounter             int
	availability                     float64
	instanceCompletedShutdownCounter int
}

// the basic server code was inspired by the tutorial https://www.youtube.com/watch?v=qJQrrscB1-4
func startupChurnController(controlAddr net.Addr, numberOfInitialBootstrappers int, environment *runtime.RunEnv) {
	environment.RecordMessage("Initialize churn controller.")
	controlServer := &controlServer{
		controlAddr:                      controlAddr,
		environment:                      environment,
		nodeInfoChannel:                  make(chan basicNodeInfo),
		establishedConnections:           make(chan net.Conn, 100), //do increase this if the instances go beyond 30 also if you have time fix route cause that this blocks programm if to small
		bootstrapInfo:                    []string{},
		globalNodeTable:                  []basicNodeInfo{},
		activeConnections:                []net.Conn{},
		IPFSCids:                         []string{},
		currentlyActiveInstances:         environment.TestInstanceCount - 1, //all active instances in the network - controller
		testEndFlag:                      false,
		churnEndFlag:                     false,
		currentlyActiveRoutines:          0,
		readyForChurnCounter:             0,
		instanceCompletedShutdownCounter: 0,
		numberOfNetworkBootstrappers:     numberOfInitialBootstrappers,
	}
	defer close(controlServer.nodeInfoChannel)
	defer close(controlServer.establishedConnections)
	environment.RecordMessage("Churn controller initialized. Begin operation.")

	controlServer.increaseActiveRoutineCounter()
	go controlServer.bootstrapAssistance()

	controlServer.increaseActiveRoutineCounter()
	go controlServer.churnControlThread()

	controlServer.increaseActiveRoutineCounter()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go controlServer.startUpListener(&waitGroup)

	waitGroup.Wait()

	environment.RecordMessage("The adress was: %v", controlServer.controlAddr.String())
	environment.RecordMessage("The network was: %v", controlServer.controlAddr.Network())
}

// the basic code how to create a listener and how to accept connections was inspired by the tutorial https://www.youtube.com/watch?v=qJQrrscB1-4
func (controlServer *controlServer) startUpListener(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	var err error
	controlServer.listener, err = net.Listen("tcp4", strings.Split(controlServer.controlAddr.String(), "/")[0]+":4500")
	if err != nil {
		controlServer.environment.RecordMessage("Failed to set up Listener.")
		return
	}
	defer controlServer.listener.Close()
	controlServer.environment.RecordMessage("Listener has been initialized successfuly. Start accepting connections.")

	for {
		connection, err := controlServer.listener.Accept()
		controlServer.readWriteSynchronisation.RLock()
		if err != nil && controlServer.testEndFlag == false {
			controlServer.readWriteSynchronisation.RUnlock()
			controlServer.environment.RecordMessage("Connection failed. The listener proceeds will proceed accepting othe messages. The error was: %v", err)
			continue
		} else if controlServer.testEndFlag {
			controlServer.readWriteSynchronisation.RUnlock()
			controlServer.environment.RecordMessage("Listener shuts down.")
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

// the basic reader code of this function was inspired by https://www.youtube.com/watch?v=qJQrrscB1-4
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
		go controlServer.controlServerMessageSlicer(string(readBuff[:numberOfMessageBytes]), connection)

	}
}

func (controlServer *controlServer) bootstrapAssistance() {

	var nodeTable []basicNodeInfo                                        //table consisting of all the network information (so all instances execpt controller)
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
	controlServer.decreaseActiveRoutineCounter()
}

func (controlServer *controlServer) writeToNodeInfoChannel(connection net.Conn, message string) {

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
		controlServer.environment.RecordMessage("Invalid Message reached Channel processing for Node Info Channel! The message was: %v", message)
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
			bootstrapInfoAsString := "??bootstrapInfo"
			for _, nextPart := range controlServer.bootstrapInfo {
				bootstrapInfoAsString = bootstrapInfoAsString + "--" + nextPart
			}
			controlServer.readWriteSynchronisation.RUnlock()
			currentConnection.Write([]byte(bootstrapInfoAsString))
			controlServer.environment.RecordMessage("Send following message to client: %v", bootstrapInfoAsString)
		}

	}
	if strings.Contains(instruction, "!!--Bootstrap information unavailable at the moment.") {
		time.Sleep(1 * time.Second)
		controlServer.requestBootstrapInfo(currentConnection)
	}
	if strings.Contains(instruction, "!!--Please send me the available CIDs in the Network.") {
		controlServer.readWriteSynchronisation.RLock()
		if len(controlServer.IPFSCids) == 0 {
			controlServer.readWriteSynchronisation.RUnlock()
			currentConnection.Write([]byte("!!--Currently no recorded CIDs."))
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
			controlServer.changeChurnEndFlag()
			for _, conn := range connSlice {
				conn.Write([]byte("??--Test ended you can close now."))
			}
			controlServer.barrierBeforeUltimateShutdown()
			controlServer.environment.RecordMessage("The test ended the Churn controller will now shut down.")
			controlServer.readWriteSynchronisation.Lock()
			controlServer.testEndFlag = true //synchronisation flag to shut down all relevant functions of the controller
			controlServer.readWriteSynchronisation.Unlock()
			controlServer.listener.Close()
			controlServer.routineServerShutdownBarrier()
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
	if strings.Contains(instruction, "!!--Shutdown finished.") {
		controlServer.barrierLock.Lock()
		controlServer.instanceCompletedShutdownCounter = controlServer.instanceCompletedShutdownCounter + 1
		controlServer.barrierLock.Unlock()
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

func (controlServer *controlServer) changeChurnEndFlag() {
	controlServer.churnEndFlagSynchronisation.Lock()
	controlServer.churnEndFlag = true
	controlServer.churnEndFlagSynchronisation.Unlock()
	return
}

func (controlServer *controlServer) barrierBeforeUltimateShutdown() {
	controlServer.barrierLock.RLock()
	for controlServer.instanceCompletedShutdownCounter < controlServer.environment.TestInstanceCount-1 {
		controlServer.barrierLock.RUnlock()
		time.Sleep(1 * time.Second)
		controlServer.barrierLock.RLock()
	}
	controlServer.barrierLock.RUnlock()
	return
}
