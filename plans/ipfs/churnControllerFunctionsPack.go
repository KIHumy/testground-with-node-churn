package main

import (
	"math"
	"math/rand"
	"net"
	"time"
)

type frequency struct {
	numberOfEvent int
	timePeriod    time.Duration
}

func (controlServer *controlServer) setChurnFrequency(numberOfEvent int, timePeriod time.Duration) {
	controlServer.churnFrequency = frequency{
		numberOfEvent: numberOfEvent,
		timePeriod:    timePeriod,
	}

}

func (controlServer *controlServer) churnControlThread() {
	controlServer.readWriteSynchronisation.RLock()
	for controlServer.readyForChurnCounter != controlServer.environment.TestInstanceCount-1 { //loop until bootstrapPhase begins
		controlServer.readWriteSynchronisation.RUnlock()
		time.Sleep(10 * time.Second)
		controlServer.readWriteSynchronisation.RLock()
	}
	controlServer.readWriteSynchronisation.RUnlock()
	numberOfEvent := controlServer.environment.IntParam("numberOfEvent")
	timePeriodString := controlServer.environment.StringParam("timePeriodString")
	timePeriod, err := time.ParseDuration(timePeriodString)
	controlServer.distribution = controlServer.environment.StringParam("distribution")
	controlServer.churnMode = controlServer.environment.StringParam("churnMode")
	controlServer.availability = controlServer.environment.FloatParam("availability")
	lambdaScaleParameter := controlServer.environment.FloatParam("lambdaScaleParameter")
	kShapeParameter := controlServer.environment.FloatParam("kShapeParameter")
	numberOfEvent, timePeriod = controlServer.inputSanitizerControlFunctions(numberOfEvent, timePeriod, err)

	controlServer.environment.RecordMessage("The availability parameter after reading the input is %v.", controlServer.availability)

	controlServer.setChurnFrequency(numberOfEvent, timePeriod)
	if controlServer.distribution == "normal" {
		controlServer.calculateRuntimeDistributionNormal()
	} else if controlServer.distribution == "weibull" {
		controlServer.calculateRuntimeDistributionWeibull(lambdaScaleParameter, kShapeParameter)
	} else {
		controlServer.environment.RecordMessage("No valid distribution churn control failed!")
		return
	}

	controlServer.signalChurnStart()
	controlServer.environment.RecordMessage("Starting Churn now.")
	churnTicker := time.NewTicker(controlServer.churnFrequency.timePeriod)
	controlServer.churnEndFlagSynchronisation.RLock()
	for controlServer.churnEndFlag == false {
		controlServer.churnEndFlagSynchronisation.RUnlock()
		changedFlag := false
		signal := <-churnTicker.C
		controlServer.churnEndFlagSynchronisation.RLock()
		if controlServer.churnEndFlag {
			continue
		}
		controlServer.environment.RecordMessage("New Churn Test after Ticker signal: %v", signal)
		for index, node := range controlServer.globalNodeTable {
			if node.churnable {
				randomNumber := rand.Float64()
				controlServer.environment.RecordMessage("The random Number for this instance is: %v and the nodeProbabilityUp is: %v and the nodeProbabilityDown is: %v", randomNumber, node.probabilityUp, node.probabilityDown)
				if (randomNumber < node.probabilityUp && node.status == "down") || (randomNumber < node.probabilityDown && node.status == "up") {
					if controlServer.churnMode == "down" { //handling of churn if mode is down
						if node.status == "up" {
							controlServer.globalNodeTable[index].status = "down"
							controlServer.sendDownSignal(node.connectionToTheNode)
							changedFlag = true
						} else {
							continue
						}
					} else if controlServer.churnMode == "downAndRecover" { //handling of churn if mode is down and recover
						if node.status == "up" {
							controlServer.globalNodeTable[index].status = "down"
							controlServer.sendDownSignal(node.connectionToTheNode)
						} else {
							controlServer.globalNodeTable[index].status = "up"
							controlServer.sendRecoverSignal(node.connectionToTheNode)
						}
					} else {
						controlServer.environment.RecordMessage("Invalid mode. Churn failed.")
						return
					}
				}
			} else {
				continue
			}
		}
		if changedFlag && controlServer.churnMode == "down" {
			if controlServer.distribution == "normal" {
				controlServer.calculateRuntimeDistributionNormal()
			} else if controlServer.distribution == "weibull" {
				controlServer.calculateRuntimeDistributionWeibull(lambdaScaleParameter, kShapeParameter)
			}
		}
	}
	churnTicker.Stop()
	controlServer.churnEndFlagSynchronisation.RUnlock()
	controlServer.decreaseActiveRoutineCounter()
	return
}

func (controlServer *controlServer) calculateAvailabilityIntoProbabilityOfInstance(probabilityOfInstance float64) (float64, float64) {
	if controlServer.churnMode == "down" {
		return 0, probabilityOfInstance
	}
	probabilityOfInstanceUp := probabilityOfInstance * 2 * controlServer.availability
	probabilityOfInstanceDown := probabilityOfInstance * 2 * (1 - controlServer.availability)
	return probabilityOfInstanceUp, probabilityOfInstanceDown
}

func (controlServer *controlServer) calculateRuntimeDistributionNormal() {
	var numberOfChurnableNodes float64
	numberOfChurnableNodes = 0
	for _, node := range controlServer.globalNodeTable {
		if node.churnable && (node.status == "down" && controlServer.churnMode == "down") == false { //if the probability for churn needs to be recalculated do not count down nodes in mode down to keep frequency of network correct
			numberOfChurnableNodes = numberOfChurnableNodes + 1
		} else {
			continue
		}
	}
	probabilityOfInstance := float64(controlServer.churnFrequency.numberOfEvent) / numberOfChurnableNodes
	probabilityOfInstanceUp, probabilityOfInstanceDown := controlServer.calculateAvailabilityIntoProbabilityOfInstance(probabilityOfInstance)
	for index, _ := range controlServer.globalNodeTable {
		if controlServer.globalNodeTable[index].churnable {
			controlServer.globalNodeTable[index].probabilityUp = probabilityOfInstanceUp
			controlServer.globalNodeTable[index].probabilityDown = probabilityOfInstanceDown
			controlServer.environment.RecordMessage("The probability of this instance is %v. Its upProbability is %v. Its downProbability is %v.", probabilityOfInstance, probabilityOfInstanceUp, probabilityOfInstanceDown)
		}
	}
	controlServer.globalNodeTableSanitizer()
	return
}

func (controlServer *controlServer) calculateRuntimeDistributionWeibull(lambdaScaleParameter float64, kShapeParameter float64) {
	var numberOfChurnableNodes float64
	numberOfChurnableNodes = 0
	for _, node := range controlServer.globalNodeTable {
		if node.churnable && (node.status == "down" && controlServer.churnMode == "down") == false {
			numberOfChurnableNodes = numberOfChurnableNodes + 1
		} else {
			continue
		}
	}
	seperationValues := 1 / numberOfChurnableNodes
	var thresholdSlice []float64
	for counter := 0; float64(counter) < numberOfChurnableNodes; counter++ {
		newHurdle := seperationValues*float64(counter) + (seperationValues / 2) //these values will be needed to seperate the churnable nodes according to the distribution function
		thresholdSlice = append(thresholdSlice, newHurdle)
	}
	var valueSlice []float64
	for _, hurdle := range thresholdSlice {
		valueOfProbability := (lambdaScaleParameter * math.Pow(-math.Log(1-hurdle), 1/kShapeParameter)) + 1 //use quantile function to find number of steps each node should make until break add 1 as offset because we cannot churn at zero one is lowest value
		valueSlice = append(valueSlice, valueOfProbability)
	}

	var protoSliceProbability []float64
	var probabilitySum float64
	var frequencyModifier float64
	for _, value := range valueSlice { //use step number to calculate proto probability. Proto value because not accounting for frequency and liveness yet.
		//Because of the restrictions of the distribution it maybe that the distribution is not correct and might cause probabilitys higher then one. liveness will be applied for network not for instance
		protoValueProbability := 1 / value
		protoSliceProbability = append(protoSliceProbability, protoValueProbability)
		probabilitySum = probabilitySum + protoValueProbability
	}
	frequencyModifier = float64(controlServer.churnFrequency.numberOfEvent) / probabilitySum
	for index, _ := range protoSliceProbability {
		controlServer.environment.RecordMessage("This is the probability before the frequency scaler: %v", protoSliceProbability[index])
		protoSliceProbability[index] = protoSliceProbability[index] * frequencyModifier
		controlServer.environment.RecordMessage("This is the probability after the frequency scaler: %v", protoSliceProbability[index])
	}
	if controlServer.checkForSliceDidntExceedOne(protoSliceProbability) {
		controlServer.environment.RecordMessage("Probability calculation was able to apply frequency Modifier sucessfully.")
	} else {
		controlServer.environment.RecordMessage("Resulting probabilities have at least one value exceeding 1. Therefore Correction modifier will be calculated.")
		correctionModifier := controlServer.calculateCorrectionModifier(protoSliceProbability, frequencyModifier)
		controlServer.environment.RecordMessage("Apply Correction Modifier. This means frequency will no longer be maintained.")
		for indexA, _ := range protoSliceProbability {
			protoSliceProbability[indexA] = protoSliceProbability[indexA] * correctionModifier
			if math.IsInf(protoSliceProbability[indexA], 1) || math.IsNaN(protoSliceProbability[indexA]) {
				protoSliceProbability[indexA] = 1
			}
			controlServer.environment.RecordMessage("The new Value is: %v", protoSliceProbability[indexA])
		}
	}

	var counterInValueSlice int
	counterInValueSlice = 0
	for indexB, _ := range controlServer.globalNodeTable {
		if controlServer.globalNodeTable[indexB].churnable == true && (controlServer.churnMode == "down" && controlServer.globalNodeTable[indexB].status == "down") == false {
			churnProbabilityUp, churnProbabilityDown := controlServer.calculateAvailabilityIntoProbabilityOfInstance(protoSliceProbability[counterInValueSlice])
			controlServer.globalNodeTable[indexB].probabilityDown = churnProbabilityDown
			controlServer.globalNodeTable[indexB].probabilityUp = churnProbabilityUp
			counterInValueSlice = counterInValueSlice + 1
			controlServer.environment.RecordMessage("ProbabilityUP is %v ProbabilityDown is %v.", churnProbabilityUp, churnProbabilityDown)
		}
	}
	controlServer.globalNodeTableSanitizer()
	return
}

func (controlServer *controlServer) sendDownSignal(currentConnection net.Conn) {
	currentConnection.Write([]byte("!!--down"))
}

func (controlServer *controlServer) sendRecoverSignal(currentConnection net.Conn) {
	currentConnection.Write([]byte("!!--recover"))
}

func (controlServer *controlServer) signalChurnStart() {
	controlServer.readWriteSynchronisation.RLock()
	for _, node := range controlServer.globalNodeTable {
		controlServer.readWriteSynchronisation.RUnlock()
		node.connectionToTheNode.Write([]byte("??--Controller is ready Churn starts."))
		controlServer.readWriteSynchronisation.RLock()
	}
	controlServer.readWriteSynchronisation.RUnlock()
}

func (controlServer *controlServer) inputSanitizerControlFunctions(numberOfEvent int, timePeriod time.Duration, errorFromParse error) (int, time.Duration) {
	if numberOfEvent < 0 {
		controlServer.environment.RecordMessage("Your input numberOfEvent was to low (lower then zero) and has been altered to 0.")
		numberOfEvent = 0
	} else if numberOfEvent > (controlServer.environment.TestInstanceCount - 1) {
		controlServer.environment.RecordMessage("Your input numberOfEvent was to high (higher than TestInstanceCount - 1) and was altered to TestInstanceCount - 1.")
		numberOfEvent = controlServer.environment.TestInstanceCount - 1
	}
	if errorFromParse != nil {
		controlServer.environment.RecordMessage("The input value timePeriod couldn't be parsed correctly because of: %v. It was changed to 30s (default value).")
		timePeriod = 30 * time.Second
	}
	if (controlServer.distribution == "normal" || controlServer.distribution == "weibull") == false {
		controlServer.environment.RecordMessage("The entered distribution is invalid (valid distributions are: normal, weibull) and was altered to normal (default).")
		controlServer.distribution = "normal"
	}
	if (controlServer.churnMode == "down" || controlServer.churnMode == "downAndRecover") == false {
		controlServer.environment.RecordMessage("The entered mode is invalid (valid modes are: down, downAndRecover) and was altered to down (default).")
	}
	if controlServer.availability > 1 {
		controlServer.environment.RecordMessage("Your entered availability exceeds 1 (should be a number between 0 and 1) and was altered to 1.")
		controlServer.availability = 1
	} else if controlServer.availability < 0 {
		controlServer.environment.RecordMessage("Your entered availability is lower than 0 (should be a number between 0 and 1) and was altered to 0.")
		controlServer.availability = 0
	}
	return numberOfEvent, timePeriod
}

func (controlServer *controlServer) checkForSliceDidntExceedOne(slice []float64) bool {
	for _, value := range slice {
		if value > 1 {
			return false
		}
	}
	return true
}

func (controlServer *controlServer) calculateCorrectionModifier(slice []float64, modifier float64) float64 {
	var maxValue float64
	for index, value := range slice {
		if index == 0 {
			maxValue = value
		}
		if value > maxValue {
			maxValue = value
		}
	}
	var newModifier float64
	if maxValue < 1 {
		return modifier
	} else {
		newModifier = 1 / maxValue
	}
	return newModifier
}

func (controlServer *controlServer) globalNodeTableSanitizer() {
	var maxValue float64
	for index, valueA := range controlServer.globalNodeTable {
		if index == 0 {
			if controlServer.globalNodeTable[index].probabilityUp > controlServer.globalNodeTable[index].probabilityDown {
				maxValue = controlServer.globalNodeTable[index].probabilityUp
			} else {
				maxValue = controlServer.globalNodeTable[index].probabilityDown
			}
		} else {
			if valueA.probabilityUp > maxValue {
				maxValue = valueA.probabilityUp
			}
			if valueA.probabilityDown > maxValue {
				maxValue = valueA.probabilityDown
			}
		}
	}
	if maxValue > 1 {
		controlServer.environment.RecordMessage("Sanitizer deteced probabilitys higher then one issuing new values. As close as possible to the original one. This step ignores the entered frequency.")
		correctionModifier := 1 / maxValue
		for indexA, _ := range controlServer.globalNodeTable {
			if controlServer.globalNodeTable[indexA].churnable {
				controlServer.globalNodeTable[indexA].probabilityUp = controlServer.globalNodeTable[indexA].probabilityUp * correctionModifier
				if math.IsInf(controlServer.globalNodeTable[indexA].probabilityUp, 1) || math.IsNaN(controlServer.globalNodeTable[indexA].probabilityUp) {
					controlServer.globalNodeTable[indexA].probabilityUp = 1
				}
				controlServer.globalNodeTable[indexA].probabilityDown = controlServer.globalNodeTable[indexA].probabilityDown * correctionModifier
				if math.IsInf(controlServer.globalNodeTable[indexA].probabilityDown, 1) || math.IsNaN(controlServer.globalNodeTable[indexA].probabilityDown) {
					controlServer.globalNodeTable[indexA].probabilityDown = 1
				}
				controlServer.environment.RecordMessage("ProbabilityUP is %v ProbabilityDown is %v.", controlServer.globalNodeTable[indexA].probabilityUp, controlServer.globalNodeTable[indexA].probabilityDown)
			}
		}
	}
}
