package NonblockingAtomicCommit

import (
	BestEffortBroadcast "SD/BEB"
	PerfectFailureDetector "SD/PFD"
	"SD/PP2PLink"
	FloodingUniformConsensus "SD/UC"
	"fmt"
	"strings"
)

type ProposeEvent struct {
	To    string
	Message string
}


type NonblockingAtomicCommit struct {
	BEB *BestEffortBroadcast.BestEffortBroadcast_Module
	UC *FloodingUniformConsensus.FloodingUniformConsensus
	PFD *PerfectFailureDetector.PerfectFailureDetector

	Propose chan ProposeEvent
	Decide chan string
	
	Addresses []string
	Address string
	Voted map[string]string
	Proposed bool
	dbg bool

	last_crashed string
}

func NewNBAC(addresses []string, _dbg bool) *NonblockingAtomicCommit {
	P2PLink := PP2PLink.NewPP2PLink(addresses[0], false)
	BEB := BestEffortBroadcast.NewBEBWithP2P(addresses[0], P2PLink, false)
	PFD := PerfectFailureDetector.NewPFDWithP2P(addresses, P2PLink, false)
	UC := FloodingUniformConsensus.NewFUCCustom(addresses, BEB, PFD, true)

	NBAC := &NonblockingAtomicCommit {
		BEB: BEB,
		PFD: PFD,
		UC: UC,

		Propose: make(chan ProposeEvent, 5),
		Decide: make(chan string, 5),

		Addresses: addresses[1:],
		Address: addresses[0],
		Voted: make(map[string]string),
		Proposed: false,
		dbg: _dbg,
		last_crashed: "",
	}

	NBAC.Start()
	return NBAC
}

func (module *NonblockingAtomicCommit) Start() {
	go func() {

		for {
			select {
				case y := <- module.PFD.Crash:
					module.HandleCrash(y)
				case y := <- module.Propose:
					module.HandlePropose(y)
				case y := <- module.BEB.Ind:
					module.HandleBEBDeliver(y)
				case y := <- module.UC.Decide:
					module.HandleDecide(y)
			}
		}
	}()
}

func (module *NonblockingAtomicCommit) HandleCrash(crash_event PerfectFailureDetector.CrashEvent) {
	if module.dbg {
		fmt.Println("[NBAC - HANDLE CRASH] DETECTED A CRASHED NODE: "+crash_event.Crashed_address)
		fmt.Printf("[NBAC - HANDLE CRASH] MODULE.PROPOSED? %t\n", module.Proposed)
	}
	
	if crash_event.Crashed_address == module.last_crashed { // ja tratei este cara...
		fmt.Printf("[NBAC - HANDLE CRASH] JÁ RECEBI ESTE CRASH: %s, VOU DESCARTAR\n", crash_event.Crashed_address)
		return
	}
	
	module.last_crashed = crash_event.Crashed_address
	if !module.Proposed {
		module.UC.Propose <- FloodingUniformConsensus.ProposeEvent{
			To: module.Address,
			Message:  "ABORT"}
			module.Proposed = true
		}
		//reenvio
		
	fmt.Printf("[NBAC - HANDLE CRASH] RETRANSMITINDO O CRASH PARA O PFD...\n")
	module.PFD.Crash <- crash_event
}

func (module *NonblockingAtomicCommit) HandlePropose(propose ProposeEvent) {
	if (module.dbg) {
		fmt.Printf("-----------------------------------------------------------\n")
		fmt.Printf("[NBAC - HANDLE PROPOSE] JUST RECEIVED A PROPOSE: %s\n", propose.Message)
		fmt.Printf("[NBAC - HANDLE PROPOSE] BROADCASTING THE PROPOSE THROUGH BEB... %s\n", propose.Message)
	}

	msg := "Propose§" + propose.Message

	module.BEB.Broadcast(
		BestEffortBroadcast.BestEffortBroadcast_Req_Message{
			Addresses: module.Addresses,
			Message: msg,
		})
}

func getMapKeys(myMap map[string]string) []string {
	keys := make([]string, 0, len(myMap))

	for key := range myMap {
		keys = append(keys, key)
	}

	return keys
}

func printMapKeys(myMap map[string]string) {
	for key := range myMap {
		fmt.Println("Key:", key)
	}
}

func (module *NonblockingAtomicCommit) HandleBEBDeliver(msg BestEffortBroadcast.BestEffortBroadcast_Ind_Message) {
	if (module.dbg) {
		fmt.Printf("-----------------------------------------------------------\n")
		fmt.Printf("[NBAC - HANDLE BEB DELIVER] JUST RECEIVED A BEB DELIVER: %s\n", msg.Message)
	}


	if strings.Contains(msg.Message, "Propose"){
		splittedMsg := strings.Split(msg.Message, "§")
		from := splittedMsg[0]
		value := splittedMsg[3]
		
		if value == "ABORT" && !module.Proposed {
			module.UC.Propose <- FloodingUniformConsensus.ProposeEvent{
				To: module.Address,
				Message:  "ABORT"}
				module.Proposed = true
				} else {
					module.Voted[from] = from

					if (module.dbg) {
						fmt.Printf("[NBAC - HANDLE BEB DELIVER] MODULE.PROPOSED? %t\n", module.Proposed)
						fmt.Printf("[NBAC - HANDLE BEB DELIVER] WHO VOTED? %v\n", getMapKeys(module.Voted))
					}
					
					if (len(module.Voted) == len(module.Addresses)) && !module.Proposed {
						module.UC.Propose <- FloodingUniformConsensus.ProposeEvent{
							To: module.Address,
							Message:  "COMMIT"}
							module.Proposed = true
						}
					}				
	} else {
		if (module.dbg) {
			fmt.Printf("[NBAC - HANDLE BEB DELIVER] NOT FOR ME, RETRANSMITTING... \n")
		}
		splittedMsg := strings.Split(msg.Message, "§")
		to := splittedMsg[1]
		module.BEB.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
			To: to,
			Message: msg.Message,
		}
	}
}

func (module *NonblockingAtomicCommit) HandleDecide(decision string) {
	if (module.dbg) {
		fmt.Printf("-----------------------------------------------------------\n")
		fmt.Printf("[NBAC - HANDLE DECIDE] JUST RECEIVED A DECIDE FROM UC: %s\n", decision)
	}

	module.Decide <- decision
}