package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/boxo/bootstrap"
	files "github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/path"
	cidTransform "github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	icore "github.com/ipfs/kubo/core/coreiface"
	"github.com/ipfs/kubo/core/node/libp2p"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/testground/sdk-go/network"
	testrun "github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
)

// Parts of this function are copied from Testground (pingpong.go).
// Source: https://github.com/testground/testground/blob/master/plans/network/pingpong.go
// License: MIT License (https://github.com/testground/testground/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/testground/testground/blob/master/LICENSE-APACHE)
// Adjustments: Changed the Routing policy in the network Config to allowAll.
// --- Begin of copied section (Testground) ---
func ipfsDemo(runenv *runtime.RunEnv, initCtx *testrun.InitContext) error { //This function organizes the test and is the first function that is called by main.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Hour)
	defer cancel()

	runenv.RecordMessage("before sync.MustBoundClient")
	client := initCtx.SyncClient
	netclient := initCtx.NetClient

	instanceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}

	config := &network.Config{
		// Control the "default" network. At the moment, this is the only network.
		Network: "default",

		// Enable this network. Setting this to false will disconnect this test
		// instance from this network. You probably don't want to do that.
		Enable: true,
		Default: network.LinkShape{
			Latency:   100 * time.Millisecond,
			Bandwidth: 1 << 20, // 1Mib
		},
		CallbackState: "network-configured",
		RoutingPolicy: network.AllowAll,
	}

	runenv.RecordMessage("before netclient.MustConfigureNetwork")
	netclient.MustConfigureNetwork(ctx, config)

	// Make sure that the IP addresses don't change unless we request it.
	if newAddrs, err := net.InterfaceAddrs(); err != nil {
		return err
	} else if !sameAddrs(instanceAddrs, newAddrs) {
		return fmt.Errorf("interfaces changed")
	}

	seq := client.MustSignalAndWait(ctx, "ip-allocation", runenv.TestInstanceCount)
	runenv.RecordMessage("I am %d", seq)

	// --- End of copied section ---

	patienceString := runenv.StringParam("patience")
	patience, errPatience := time.ParseDuration(patienceString)
	patienceEnabled := runenv.BooleanParam("patienceEnabled")
	var countBootstrapPeers int64
	countBootstrapPeers = int64(runenv.IntParam("countBootstrapPeers"))
	var churnable string
	numberOfRecords := runenv.IntParam("numberOfRecords")
	bootstrappersSuceptibleToChurn := runenv.BooleanParam("bootstrappersSuceptibleToChurn")
	clientSuceptibleToChurn := runenv.BooleanParam("clientSuceptibleToChurn")
	downloadTimeoutString := runenv.StringParam("downloadTimeout")
	downloadTimeout, err := time.ParseDuration(downloadTimeoutString)
	countBootstrapPeers, numberOfRecords, downloadTimeout, patience = sanitizeIPFSInputs(countBootstrapPeers, numberOfRecords, downloadTimeout, err, patience, errPatience, runenv)

	testNetIP, controlNetIP, rolePrefix := whichIpIsMyNetwork(instanceAddrs, netclient, runenv)
	runenv.RecordMessage("Normal IP: %v", testNetIP.String())
	runenv.RecordMessage("Control IP: %v", controlNetIP.String())
	myRole := whatRoleAmI(rolePrefix, testNetIP, countBootstrapPeers, runenv) //check if this instance is a bootstrap node

	if myRole == "bootstrapper" {
		if bootstrappersSuceptibleToChurn {
			churnable = "true"
		} else {
			churnable = "false"
		}
	} else {
		if clientSuceptibleToChurn {
			churnable = "true"
		} else {
			churnable = "false"
		}

	}

	var connectionToControler net.Conn
	if myRole != "churn controller" {
		connectionEstablished := false
		for connectionEstablished == false {
			connectionToControler, err = connectToChurnController(testNetIP) //if node is not churn controller connect to it
			if err != nil {
				runenv.RecordMessage("I was unable to connect to churn controller because: %v", err)
				connectionEstablished = false
			} else {
				connectionEstablished = true
			}
		}

		//initialization of the client control and usage. This startup Process is not in the respective file because the
		//client needs to have access to the clientControl object to access it's functions.
		clientControlInstance := initiateNewClientControl(connectionToControler, myRole, patience, patienceEnabled, "IPFS", runenv)
		clientControlInstance.increaseActiveRoutineCounter()
		go clientControlInstance.clientChurnReader()
		//end of the clientControl initialization

		clientControlInstance.communicateNodeInfoToController(myRole, churnable)

		//spawn one ipfs-node per testground-instance
		clientControlInstance.setNumberOfIPFSRecords(numberOfRecords)
		ipfsContext, endTheMainFunction := context.WithCancel(ctx)
		apiOfIPFSNode, node, _ := spawnEphemeral(ipfsContext, runenv, clientControlInstance, testNetIP)
		generateFilesForRepo(runenv, ipfsContext, apiOfIPFSNode, rolePrefix, clientControlInstance, numberOfRecords)
		clientControlInstance.setRolePrefix(rolePrefix)

		clientControlInstance.setMotherContext(ctx) //this is needed to generate new working context with cancel in recovery step
		clientControlInstance.setCoreIPFSNode(node) //give pointer to node to clientControlInstance so churn handler can work on it
		clientControlInstance.setIPFSAPI(apiOfIPFSNode)
		clientControlInstance.setCancelContext(ipfsContext, endTheMainFunction)
		clientControlInstance.increaseActiveRoutineCounter()
		go clientControlInstance.churnHandler()
		bootstrapConfig, err := generateNewBootstrapRoutine(clientControlInstance) //generates BootstrapRoutine
		err = node.Bootstrap(bootstrapConfig)                                      //executes Bootstrap
		if err != nil {
			runenv.RecordMessage("Failed during Bootstrap with: %v", err)
		} else {
			runenv.RecordMessage("Bootstrap was successfull.")
			runenv.RecordMessage("I connected to %v", len(node.PeerHost.Network().Peers()))
		}

		time.Sleep(30 * time.Second)
		runenv.RecordMessage("I connected later to %v", len(node.PeerHost.Network().Peers()))

		newRandomizer := rand.New(rand.NewSource(seq))
		outputDirectoryNumber := 0
		clientControlInstance.reportInstanceReadyForChurn()
		clientControlInstance.barrierBeforeChurn()
		clientControlInstance.startFrustrationTimer()
		for {
			responseString := clientNetworkBehaviour(clientControlInstance, runenv, clientControlInstance.clientIPFSNode, clientControlInstance.context, clientControlInstance.nodeAPI, outputDirectoryNumber, newRandomizer, downloadTimeout)
			if responseString == "Finished plan." || (responseString == "canceled" && clientControlInstance.churnMode == "down") || clientControlInstance.checkFrustration() {
				clientControlInstance.announcePlanFinished()
				clientControlInstance.awaitCloseSignalAndCloseClientControl()
				select {
				case <-ctx.Done():
					runenv.RecordMessage("Context already canceled node should be closed.")
				default:
					clientControlInstance.contextCancelFunc()
					for _, connection := range node.PeerHost.Network().Conns() {
						connection.Close()
					}
					err := node.PeerHost.Close()
					if err != nil {
						runenv.RecordMessage("PeerHost closure failed with: %v", err)
					}
					err = node.DHT.Close()
					if err != nil {
						runenv.RecordMessage("PeerHost closure failed with: %v", err)
					}
					err = node.Close()
					if err != nil {
						runenv.RecordMessage("Peer closure failed with: %v", err)
					}
				}
				runenv.RecordMessage("Instance finished the plan.")
				return nil
			}
			if responseString == "canceled" || responseString == "failure" {
				continue
			} else {
				outputDirectoryNumber = outputDirectoryNumber + 1
				clientControlInstance.resetFrustration()
			}
		}
	}
	if myRole == "churn controller" {
		startupChurnController(testNetIP, int(countBootstrapPeers), runenv)
	}

	return nil
}

// This function is copied from Testground (pingpong.go).
// Source: https://github.com/testground/testground/blob/master/plans/network/pingpong.go
// License: MIT License (https://github.com/testground/testground/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/testground/testground/blob/master/LICENSE-APACHE)
// Adjustments: Added a comment to explain what the function does.
func sameAddrs(a, b []net.Addr) bool { //compares two addresses and returns false if they are not equal.
	if len(a) != len(b) {
		return false
	}
	aset := make(map[string]bool, len(a))
	for _, addr := range a {
		aset[addr.String()] = true
	}
	for _, addr := range b {
		if !aset[addr.String()] {
			return false
		}
	}
	return true
}

// This function is copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: none
func setupPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

// Parts of this function are copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: Changed code for clean integration with Testground in the copied part. Changed some error messages.
// --- Begin of copied section (IPFS Kubo project) ---
func createTempRepo(runenv *runtime.RunEnv, clientControl *clientControl, testNetIP net.Addr) (string, error) { //Creates a repo for the IPFS node.
	repoPath, err := os.MkdirTemp("", "ipfs-shell-*")
	if err != nil {
		runenv.RecordMessage("Failed to get temp dir: %v", err)
	}
	cfg, err := config.Init(io.Discard, 2048)
	if err != nil {
		runenv.RecordMessage("Failed to generate default config: %v", err)
	}

	// --- End of copied section ---

	cfg.Swarm.DisableNatPortMap = true //makes the program unable to connect to the lokal hardware to forward traffic

	newPort := fmt.Sprintf("%v", clientControl.ipfsPortCounter)
	cfg.Addresses.Swarm = []string{"/ip4/0.0.0.0/tcp/" + newPort}

	announceString := "/ip4/" + strings.Split(strings.Split(testNetIP.String(), ":")[0], "/")[0] + "/tcp/" + newPort
	bootstrapString := announceString + "/p2p/" + cfg.Identity.PeerID

	clientControl.incrementPortNumber()

	clientControl.setClientControlBootstrapInformation(bootstrapString)
	runenv.RecordMessage("This is the clientBootstrapInformation %v.", clientControl.clientBootstrapInformation)
	runenv.RecordMessage("This is my announce String: %v", announceString)
	cfg.Addresses.Announce = append(cfg.Addresses.Announce, announceString)

	if clientControl.myRole == "bootstrapper" {
		clientControl.updateMyBootstrapInformation()
	}
	clientControl.requestNetworkBootstrapInfo()
	clientControl.setNetworkBootstrapInformation()
	cfg.Bootstrap = clientControl.returnTheNetworkBootstrappers()

	// --- Begin of copied section (IPFS Kubo project) ---

	err = fsrepo.Init(repoPath, cfg)
	if err != nil {
		runenv.RecordMessage("Initializing fsrepo failed with %v:", err)
	}

	return repoPath, nil
} // --- End of copied section ---

// This function is copied from the IPFS Kubo project and was then slightly modified.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: Changed code for clean integration with Testground. Added some additional recording to make the process more transparent.
// Creates an IPFS node and returns its coreAPI.
func createNode(ctx context.Context, repoPath string, runenv *runtime.RunEnv) (*core.IpfsNode, error) {
	//Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		runenv.RecordMessage("Opening of repo failed with: %v", err)
	}

	// Construct the node

	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: libp2p.DHTOption, // This option sets the node to be a full DHT node (both fetching and storing DHT Records)
		Repo:    repo,
	}
	runenv.RecordMessage("Finished initialization of the config.")

	constructedNode, err := core.NewNode(ctx, nodeOptions) //define output as variable to be able to publish information regarding the node
	if err != nil {
		runenv.RecordMessage("Failed to create the new node: %v", err)
	}

	runenv.RecordMessage("Finished config of node.")

	return constructedNode, err
}

// This statement is copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: none
var loadPluginsOnce sync.Once

// This function is copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: Added some additional recording to make the process more transparent. Set the repo path in clientControl.
// Spawns a node to be used just for this run (i.e. creates a tmp repo).
func spawnEphemeral(ctx context.Context, runenv *runtime.RunEnv, clientControl *clientControl, testNetIP net.Addr) (icore.CoreAPI, *core.IpfsNode, error) { //added runenv to the input parameters
	var onceErr error
	loadPluginsOnce.Do(func() {
		onceErr = setupPlugins("")
	})
	if onceErr != nil {
		runenv.RecordMessage("Loading Plugins failed with: %v", onceErr)
	}

	// Create a Temporary Repo
	repoPath, err := createTempRepo(runenv, clientControl, testNetIP) //renamed variable to make the code more readable. Added runenv as input parameter
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp repo: %s", err)
	}
	runenv.RecordMessage("Finished repo Initialisation.")

	clientControl.setipfsRepoPath(repoPath)

	runenv.RecordMessage("Begin node construction!")
	node, err := createNode(ctx, repoPath, runenv) //renamed variable to make the code more readable
	if err != nil {
		return nil, nil, err
	}
	runenv.RecordMessage("Finished node construction.")
	api, err := coreapi.NewCoreAPI(node)

	return api, node, err
}

// This function is copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: Changed code for clean integration with Testground. Made the function case sensitive meaning if the context is canceled function exits immediately.
// A second return value was added to make the circumstances of the return clear to other functions. Added a success message to make results more transparent.
func connectToPeers(runenv *runtime.RunEnv, ctx context.Context, ipfs icore.CoreAPI, peers []string) (error, string) { //added runenv to this funktion
	select {
	case <-ctx.Done():
		runenv.RecordMessage("Context has been canceled connectToPeers function exits.")
		return nil, "canceled"
	default:
		var wg sync.WaitGroup
		peerInfos := make(map[peer.ID]*peer.AddrInfo, len(peers))
		for _, addrStr := range peers {
			addr, err := ma.NewMultiaddr(addrStr)
			if err != nil {
				return err, ""
			}
			pii, err := peer.AddrInfoFromP2pAddr(addr)
			if err != nil {
				return err, ""
			}
			pi, ok := peerInfos[pii.ID]
			if !ok {
				pi = &peer.AddrInfo{ID: pii.ID}
				peerInfos[pi.ID] = pi
			}
			pi.Addrs = append(pi.Addrs, pii.Addrs...)
		}

		var couldYouReachSomething bool
		couldYouReachSomething = false
		wg.Add(len(peerInfos))
		for _, peerInfo := range peerInfos {
			go func(peerInfo *peer.AddrInfo) {
				defer wg.Done()
				err := ipfs.Swarm().Connect(ctx, *peerInfo)
				if err != nil {
					runenv.RecordMessage("failed to connect to %s: %s", peerInfo.ID, err)
				} else {
					runenv.RecordMessage("Connected to %s.", peerInfo.ID)
					couldYouReachSomething = true
				}
			}(peerInfo)
		}
		wg.Wait()
		if couldYouReachSomething {
			return nil, ""
		} else {
			return nil, "unableToConnect"
		}

	}

}

// This function is copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: The function was modified in one way the function files.NewSerialFile(...) and files.Node are now imported from a different package.
func getUnixfsNode(path string) (files.Node, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := files.NewSerialFile(path, false, st)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func whatRoleAmI(rolePrefix string, testNetIP net.Addr, countBootstrapPeers int64, runenv *runtime.RunEnv) string { //Decides the role a node has.

	controlIPIndex, _ := strconv.ParseInt(strings.Split(strings.Split(testNetIP.String(), "/")[0], ".")[3], 10, 64)

	if controlIPIndex == 2 {
		runenv.RecordMessage("I am the churn controller. My IP is %v.", testNetIP.String())
		return "churn controller"
	} else if controlIPIndex <= 2+countBootstrapPeers {
		runenv.RecordMessage("I am a bootstrapper. My IP is %v.", testNetIP.String())
		return "bootstrapper"
	} else {
		runenv.RecordMessage("I am normal. My IP is %v.", testNetIP.String())
		return "normal"
	}
}

func whichIpIsMyNetwork(adressesOfNetwork []net.Addr, netClient *network.Client, runenv *runtime.RunEnv) (net.Addr, net.Addr, string) { //Function to get the data network IP the control network IP and the role Prefix.

	var ownControlNetworkAdress net.Addr
	var ownNetworkAdress net.Addr
	var rolePrefix string
	outputValue, _ := netClient.GetDataNetworkIP()
	ownNetworkAdressString := outputValue.String()
	for _, adress := range adressesOfNetwork {
		splittedAdress := strings.Split(adress.String(), "/")
		praefixLength := splittedAdress[len(splittedAdress)-1]
		if splittedAdress[0] == ownNetworkAdressString {
			ownNetworkAdress = adress
			rolePrefix = strings.Split(splittedAdress[0], ".")[3]
		}
		if praefixLength == "16" && (strings.Split(splittedAdress[0], ".")[0] == "192" && strings.Split(splittedAdress[0], ".")[1] == "18") {
			ownControlNetworkAdress = adress
		}
	}
	runenv.RecordMessage("Adress in Data Network is: %v.", ownNetworkAdress)
	runenv.RecordMessage("Adress in Control Network is: %v", ownControlNetworkAdress)
	return ownNetworkAdress, ownControlNetworkAdress, rolePrefix
}

func connectToChurnController(testNetIP net.Addr) (net.Conn, error) { //function that connects an instance to the churn controller.

	seperatedIP := strings.Split(strings.Split(testNetIP.String(), "/")[0], ".")
	controlerAdressWithPort := seperatedIP[0] + "." + seperatedIP[1] + ".0.2:4500"
	connectionToController, err := net.Dial("tcp4", controlerAdressWithPort)

	return connectionToController, err
}

func generateNewBootstrapRoutine(clientControl *clientControl) (bootstrap.BootstrapConfig, error) { //Generates a bootstrap config for the bootstrap process of an IPFS node.
	networkBootstrapInformation := clientControl.returnTheNetworkBootstrappers()
	var multiAddrSlice []ma.Multiaddr
	for _, adress := range networkBootstrapInformation {
		multiAddr, err := ma.NewMultiaddr(adress)
		if err != nil {
			clientControl.environment.RecordMessage("Failed while parsing string to Multiaddr with: %v", err)
			continue
		}
		multiAddrSlice = append(multiAddrSlice, multiAddr)
	}
	addrInfoSliceOfBootstrappers, err := peer.AddrInfosFromP2pAddrs(multiAddrSlice...)
	if err != nil {
		clientControl.environment.RecordMessage("Failed during conversion to peer.AddrInfo with: %v", err)
	}
	bootstrapConfig := bootstrap.BootstrapConfigWithPeers(addrInfoSliceOfBootstrappers)
	bootstrapConfig.MinPeerThreshold = 3
	return bootstrapConfig, err
}

func generateFilesForRepo(runenv *runtime.RunEnv, ctx context.Context, nodeIPFSApi icore.CoreAPI, rolePrefix string, clientControl *clientControl, numberOfRecords int) { //Generates the files that should be provided over the IPFS network and adds them to the IPFS repo.

	prefixInt, err := strconv.Atoi(rolePrefix)
	var repoData []string
	for counter := 0; counter < runenv.TestInstanceCount; counter++ {
		data := fmt.Sprintf("%v", counter)
		exampleDataString := strings.Repeat(data, 10000)
		repoData = append(repoData, exampleDataString)
	}
	if err != nil {
		runenv.RecordMessage("String conversion of rolePrefix failed.")
	}
	newDirectory, err := os.MkdirTemp("", "repo-data")
	if err != nil {
		runenv.RecordMessage("Creating new directory failed.")
	}

	var ipfsRecordNumber []int
	for counterA := 0; counterA < numberOfRecords; counterA++ {
		newValue := prefixInt + counterA
		if newValue > runenv.TestInstanceCount-1 {
			newValue = newValue - (runenv.TestInstanceCount - 1)
		}
		ipfsRecordNumber = append(ipfsRecordNumber, newValue)
	}

	var dataPaths []string
	for index, dataString := range repoData {
		if slices.Contains(ipfsRecordNumber, index) {
			nameString := filepath.Join(newDirectory, fmt.Sprintf("%v", index)+".txt")
			err := os.WriteFile(nameString, []byte(dataString), 0644)
			if err != nil {
				runenv.RecordMessage("writing of File Failed.")
				continue
			}
			dataPaths = append(dataPaths, nameString)
		} else {
			continue
		}
	}
	var cidInputSlice []string
	for _, path := range dataPaths {

		// The following code is copied from the IPFS Kubo project.
		// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
		// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
		// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
		// Changes: Some variable names where modified. The recorded messages and when and if a success message is displayed has been changed.
		fileToAppend, err := getUnixfsNode(path)
		if err != nil {
			runenv.RecordMessage("couldn't get the File")
		}
		cidFile, err := nodeIPFSApi.Unixfs().Add(ctx, fileToAppend)
		if err != nil {
			runenv.RecordMessage("Adding CidFile to repo of ipfs node failed.")
		} else {
			runenv.RecordMessage("Added CIDFile to directory: %v", cidFile.String())
		}
		// --- End of copied section ---

		cidInputSlice = append(cidInputSlice, cidFile.String())
	}
	clientControl.setClientCids(cidInputSlice)
	clientControl.gossipIPFSCids()
}

// Large parts of this function are copied from the IPFS Kubo project.
// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
// Adjustments: Changed code for clean integration with Testground. Renamed some variables. Made the success message conditional. Added a download timeout.
func getCidFileFromIPFSNetwork(runenv *runtime.RunEnv, peerCidFile path.ImmutablePath, outputBasePath string, localNode icore.CoreAPI, ctx context.Context, downloadTimeout time.Duration) string { //basically a function to retrieve a cidFile from the network, the function is based on the main function of https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
	select {
	case <-ctx.Done():
		runenv.RecordMessage("Couldn't get CID from network context has been canceled.")
		return "canceled"
	default:

		// --- Begin of copied section (IPFS Kubo project) ---

		exampleCIDStr := peerCidFile.RootCid().String()

		runenv.RecordMessage("Fetching a file from the network with CID %v", exampleCIDStr)
		outputPath := outputBasePath + exampleCIDStr
		testCID := path.FromCid(peerCidFile.RootCid())

		newTimeoutContext, cancel := context.WithTimeout(ctx, downloadTimeout)
		defer cancel()

		rootNode, err := localNode.Unixfs().Get(newTimeoutContext, testCID)
		if err != nil {
			runenv.RecordMessage("could not get file with CID: %v with error: %v", exampleCIDStr, err)
			return "failure"
		}

		err = files.WriteTo(rootNode, outputPath)
		if err != nil {
			runenv.RecordMessage("could not write out the fetched CID: %v with error: %v", exampleCIDStr, err)
			return "failure"
		} else {
			runenv.RecordMessage("Wrote the file to %v", outputPath)
		}

		// --- End of copied section ---

		return ""
	}
}

func lookUpProviders(runenv *runtime.RunEnv, node *core.IpfsNode, cidAsString string, ctx context.Context) ([]string, string) { //A function to find the providers of a CID.
	select {
	case <-ctx.Done():
		runenv.RecordMessage("Lookup Providers stopped context canceled.")
		return []string{}, "canceled"
	default:
		var providers []string
		cid, err := cidTransform.Parse(cidAsString)
		if err != nil {
			runenv.RecordMessage("Cid parsing failed with: %v", err)
		}

		for addrInfo := range node.DHT.FindProvidersAsync(ctx, cid, 3) {
			collectionOfMultiaddrs := addrInfo.Addrs
			for _, multiaddrs := range collectionOfMultiaddrs {
				providerString := multiaddrs.String()
				providers = append(providers, providerString)
			}
		}
		if len(providers) == 0 {
			runenv.RecordMessage("Couldn't find a valid provider for cid %v. Probably all providers currently inactive.", cidAsString)
			return providers, "failure"
		}
		return providers, ""
	}

}

// This function defines the behaviour of the client during the test. The client downloads records that are available in the network and continues based on the success of that.
func clientNetworkBehaviour(clientControl *clientControl, runenv *runtime.RunEnv, node *core.IpfsNode, ctx context.Context, ipfsAPI icore.CoreAPI, outPutDirectoryNumber int, newRandomizer *rand.Rand, downloadTimeout time.Duration) string {
	select {
	case <-ctx.Done():
		return "canceled"
	default:
		directoryPrefix := fmt.Sprintf("%voutputDirectory", outPutDirectoryNumber)

		// The following code is copied from the IPFS Kubo project.
		// Source: https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go
		// License: MIT License (https://github.com/ipfs/kubo/blob/master/LICENSE-MIT)
		// License: Apache License (https://github.com/ipfs/kubo/blob/master/LICENSE-APACHE)
		// Changes: Changed one input to os.MkdirTemp(...). The error handling and the error message was changed. In the second part in this function the variable names and the inputs were changed.
		// --- Begin of copied section (IPFS Kubo project) ---
		outPutBasePath, err := os.MkdirTemp("", directoryPrefix)
		if err != nil {
			runenv.RecordMessage("Failed to create output Directory.")
		}
		// --- End of copied section ---

		clientControl.requestNetCids()
		clientControl.setNetCids()
		netcids := clientControl.returnNetCids()
		var cidsNotCurrentlyPresentAtInstance []string
		var nothingToAppend bool
		nothingToAppend = true //set default to true if nothing is appended
		for _, cidString := range netcids {
			if slices.Contains(clientControl.returnInstanceCids(), cidString) {
				continue
			} else {
				nothingToAppend = false //something was appended so variable is set to false
				cidsNotCurrentlyPresentAtInstance = append(cidsNotCurrentlyPresentAtInstance, cidString)
			}
		}
		if nothingToAppend { //return if nothing is left to download
			return "Finished plan."
		}

		runenv.RecordMessage("These are the cids which are not currently present at the instance: %v", cidsNotCurrentlyPresentAtInstance)
		var downloadedIndex int
		if len(cidsNotCurrentlyPresentAtInstance) <= 1 {
			downloadedIndex = 0
		} else {
			downloadedIndex = newRandomizer.Intn(len(cidsNotCurrentlyPresentAtInstance) - 1)
		}
		instanceToBeDownloaded := cidsNotCurrentlyPresentAtInstance[downloadedIndex]
		clientControl.clientChurnSynchronisation.Lock()
		providers, result := lookUpProviders(runenv, node, instanceToBeDownloaded, ctx)
		if result == "canceled" || result == "failure" {
			os.RemoveAll(outPutBasePath)
			clientControl.clientChurnSynchronisation.Unlock()
			return result
		}
		clientControl.clientChurnSynchronisation.Unlock()

		clientControl.clientChurnSynchronisation.Lock()
		_, result = connectToPeers(runenv, ctx, ipfsAPI, providers)
		if result == "canceled" {
			os.RemoveAll(outPutBasePath)
			clientControl.clientChurnSynchronisation.Unlock()
			return result
		}
		clientControl.clientChurnSynchronisation.Unlock()

		generatedCidFromString, err := cidTransform.Parse(instanceToBeDownloaded)
		if err != nil {
			runenv.RecordMessage("String conversion to cid failed in clientNetworkBehaviour with: %v", err)
		}

		// --- Begin of copied section (IPFS Kubo project) ---
		cidImmutablePath := path.FromCid(generatedCidFromString)
		// --- End of copied section ---

		clientControl.clientChurnSynchronisation.Lock()
		result = getCidFileFromIPFSNetwork(runenv, cidImmutablePath, outPutBasePath, ipfsAPI, ctx, downloadTimeout)
		if result == "canceled" || result == "failure" {
			os.RemoveAll(outPutBasePath)
			clientControl.clientChurnSynchronisation.Unlock()
			return result
		}
		clientControl.clientChurnSynchronisation.Unlock()

		newInstanceCids := []string{}
		newInstanceCids = append(newInstanceCids, instanceToBeDownloaded)
		clientControl.setClientCids(newInstanceCids)
		return ""
	}

}

func customIPFSNetworkBootstrap(runenv *runtime.RunEnv, clientControl *clientControl, nodeAPI icore.CoreAPI, ctx context.Context) string { //A function that does the connections to all the bootstrappers but no other nodes
	_, result := connectToPeers(runenv, ctx, nodeAPI, clientControl.returnTheNetworkBootstrappers())
	if result == "unableToConnect" {
		return "bootstrapFailed"
	}
	return ""
}

// This functions ensures that no invalid inputs are provided to the test plan.
func sanitizeIPFSInputs(countBootstrapPeers int64, numberOfRecords int, downloadTimeout time.Duration, err error, patience time.Duration, patienceError error, runenv *runtime.RunEnv) (int64, int, time.Duration, time.Duration) {
	if countBootstrapPeers < 1 {
		runenv.RecordMessage("The number of bootstrap peers is lower then 1 the network will not work with that and the number was altered to 1.")
		countBootstrapPeers = 1
	} else if countBootstrapPeers > (int64(runenv.TestInstanceCount) - 1) {
		runenv.RecordMessage("The number of bootstrap peers is higher then the number of network participants the value will be altered to TestInstanceCount - 1.")
		countBootstrapPeers = int64(runenv.TestInstanceCount) - 1
	}
	if numberOfRecords > (runenv.TestInstanceCount - 1) {
		runenv.RecordMessage("The numberOfRecords was to high the value will be altered to TestInstanceCount - 1. This means already that every node has every record.")
		numberOfRecords = runenv.TestInstanceCount - 1
	} else if numberOfRecords < 0 {
		runenv.RecordMessage("The numberOfRecords is below 0 so it will be altered to 0.")
		numberOfRecords = 0
	}
	if err != nil {
		runenv.RecordMessage("String parsing failed downloadTimeout input value didn't seem to work so will be replaced with 30s (default value).")
		downloadTimeout = 30 * time.Second
	}
	if patienceError != nil {
		runenv.RecordMessage("String parsing failed patience input vlaue didn't seem to work so will be replaced with 5m (default value).")
		patience = 5 * time.Minute
	}
	return countBootstrapPeers, numberOfRecords, downloadTimeout, patience
}
