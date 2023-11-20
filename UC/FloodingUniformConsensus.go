/*
  Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
  Professor: Fernando Dotti  (https://fldotti.github.io/)
  Modulo representando Perfect Point to Point Links tal como definido em:
    Introduction to Reliable and Secure Distributed Programming
    Christian Cachin, Rachid Gerraoui, Luis Rodrigues
  * Semestre 2018/2 - Primeira versao.  Estudantes:  Andre Antonitsch e Rafael Copstein
  * Semestre 2019/1 - Reaproveita conexões TCP já abertas - Estudantes: Vinicius Sesti e Gabriel Waengertner
  * Semestre 2020/1 - Separa mensagens de qualquer tamanho atee 4 digitos.
  Sender envia tamanho no formato 4 digitos (preenche com 0s a esquerda)
  Receiver recebe 4 digitos, calcula tamanho do buffer a receber,
  e recebe com io.ReadFull o tamanho informado - Dotti
  * Semestre 2022/1 - melhorias eliminando retorno de erro aos canais superiores.
  se conexao fecha nao retorna nada.   melhorias em comentarios.   adicionado modo debug. - Dotti
*/

package FloodingUniformConsensus

import (
	BestEffortBroadcast "SD/BEB"
	PerfectFailureDetector "SD/PFD"
	"SD/PP2PLink"
	"fmt"
	"strconv"
	"strings"
)

type ProposeEvent struct {
	To    string
	Message string
}

type DecideEvent struct {
	From    string
	Message string
}

type FloodingUniformConsensus struct {
	Propose chan ProposeEvent
	Decide chan string
	BEB *BestEffortBroadcast.BestEffortBroadcast_Module
	PFD *PerfectFailureDetector.PerfectFailureDetector
	Addresses []string
	Address string
	Correct map[string]string
	Round int
	Decision string
	ProposalSet map[string]string
	ReceivedFrom map[string]string
	dbg bool

}


func NewFUC(addresses []string, _dbg bool) *FloodingUniformConsensus {
	P2PLink := PP2PLink.NewPP2PLink(addresses[0], false)
	fuc := &FloodingUniformConsensus{
		Propose: make(chan ProposeEvent, 5),
		Decide: make(chan string, 5),
		BEB: BestEffortBroadcast.NewBEBWithP2P(addresses[0], P2PLink, false),
		PFD: PerfectFailureDetector.NewPFDWithP2P(addresses, P2PLink, false),
		Addresses: addresses[1:],
		Address: addresses[0],
		Correct: make(map[string]string),
		Round: 1,
		Decision: "",
		ProposalSet: make(map[string]string),
		ReceivedFrom: make(map[string]string),
		dbg: _dbg,
	}
	fuc.InitCorrect()
	fuc.Start()
	return fuc
}

func NewFUCCustom(addresses []string, BEB *BestEffortBroadcast.BestEffortBroadcast_Module, 
	PFD * PerfectFailureDetector.PerfectFailureDetector, _dbg bool) *FloodingUniformConsensus {
	fuc := &FloodingUniformConsensus{
		Propose: make(chan ProposeEvent, 5),
		Decide: make(chan string, 5),
		BEB: BEB,
		PFD: PFD,
		Addresses: addresses[1:],
		Address: addresses[0],
		Correct: make(map[string]string),
		Round: 1,
		Decision: "",
		ProposalSet: make(map[string]string),
		ReceivedFrom: make(map[string]string),
		dbg: _dbg,
	}
	fuc.InitCorrect()
	fuc.Start()
	return fuc
}

func (module *FloodingUniformConsensus) InitCorrect() {
	for i := 0; i < len(module.Addresses); i++ {
		module.Correct[module.Addresses[i]] = module.Addresses[i]
	}
}

func (module *FloodingUniformConsensus) Start() {
	go func() {

		for {
			select {
			case y := <- module.PFD.Crash:
				module.HandleCrash(y)
			case y := <- module.Propose:
				module.HandlePropose(y)
			case y := <- module.BEB.Ind:
				module.HandleBEBDeliver(y)
			}
			module.Guard()//verificar se funciona isso!
			}
		}()
}

func getMapKeys(myMap map[string]string) []string {
	keys := make([]string, 0, len(myMap))

	for key := range myMap {
		keys = append(keys, key)
	}

	return keys
}

func (module *FloodingUniformConsensus) HandlePropose(propose ProposeEvent) {

	
	module.ProposalSet[propose.Message] = propose.Message // aqui vem só a msg do valor proposto!
	if (module.dbg) {
		fmt.Printf("-----------------------------------------------------------\n")
		fmt.Printf("[UC - HANDLE PROPOSE] JUST RECEIVED A PROPOSE: %s\n", propose.Message)
		fmt.Printf("[UC - HANDLE PROPOSE] BROADCASTING THE PROPOSE THROUGH BEB... %s\n", propose.Message)
	}

	msg :=  "Proposal"+ "§1§" + concatenateValues(module.ProposalSet)
	message := BestEffortBroadcast.BestEffortBroadcast_Req_Message {
		Addresses: getMapKeys(module.Correct),
		Message: msg,
	}
	module.BEB.Broadcast(message)

}

func (module *FloodingUniformConsensus) HandleCrash(crash_event PerfectFailureDetector.CrashEvent) {
	fmt.Printf("-----------------------------------------------------------\n")
	fmt.Println("[UC - HANDLE CRASH] DETECTED A CRASHED NODE: "+crash_event.Crashed_address)
	delete(module.Correct, crash_event.Crashed_address) // retira o crashado dos corretos
	fmt.Println("[UC - HANDLE CRASH] MY CORRECTS: ")
	printMapKeys(module.Correct)
	fmt.Println("[UC - HANDLE CRASH] RETRANSMITTING...")
	module.PFD.Crash <- crash_event
}

func (module *FloodingUniformConsensus) HandleBEBDeliver (msg BestEffortBroadcast.BestEffortBroadcast_Ind_Message) {
	
	if (module.dbg) {
		fmt.Printf("-----------------------------------------------------------\n")
		fmt.Printf("[UC - HANDLE BEB DELIVER] JUST RECEIVED A BEB DELIVER: %s\n", msg.Message)
	}

	if strings.Contains(msg.Message, "Proposal"){
		splittedMsg := strings.Split(msg.Message, "§")
		msgRound, _ := strconv.Atoi(splittedMsg[3])
		fmt.Printf("[UC - HANDLE BEB DELIVER] RECEIVED ROUND: %d, MY ROUND: %d\n", msgRound, module.Round)
		if (module.Round == msgRound) {
			from := splittedMsg[0]
			module.ReceivedFrom[from] = from
			module.AppendProposed(splittedMsg[4:])
			fmt.Printf("[UC - HANDLE BEB DELIVER MODULE.ROUND == MSG_ROUNG, RECEIVED FROM: %v\n", getMapKeys(module.ReceivedFrom))
		} else {
			fmt.Println("[UC - HANDLE BEB DELIVER] DELIVER DISCARDED, BECAUSE MY ROUND != MESSAGE ROUND!")
		}
	} else if strings.Contains(msg.Message, "Propose") {
		fmt.Println("[UC - HANDLE BEB DELIVER] BEB DELIVER NOT FOR ME, RETRANSMITTING...")
		splittedMsg := strings.Split(msg.Message, "§")

		to := splittedMsg[1]

		module.BEB.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
			To: to,
			Message: msg.Message,
		}

	}
}

func printMapKeys(myMap map[string]string) {
	for key := range myMap {
		fmt.Println("Key:", key)
	}
}

func (module *FloodingUniformConsensus) Guard() {
	if isSubset(module.Correct, module.ReceivedFrom) && module.Decision == "" {
		if (module.dbg) {
			fmt.Printf("-----------------------------------------------------------\n")

			fmt.Printf("[GUARD] GUARD ACTIVATED, Correct ⊆ ReceivedFrom ? %t\n", isSubset(module.Correct, module.ReceivedFrom))
			fmt.Printf("[GUARD] CORRECT SET: %v\n", getMapKeys(module.Correct))
			fmt.Printf("[GUARD] RECEIVED FROM SET: %v\n", getMapKeys(module.ReceivedFrom))
		}
		if module.Round == len(module.Addresses) { // nao sei se ta certo esse N
			module.Decision = GetMin(getKeys(module.ProposalSet))
			fmt.Printf("[GUARD] DECISION, VALUE DECIDED: %s\n", module.Decision)
			module.Decide <- module.Decision
		} else {
			module.Round +=1
			real_msg := "Proposal§" + strconv.Itoa(module.Round) + "§" + concatenateValues(module.ProposalSet)
			module.ReceivedFrom = make(map[string]string)
			msg := BestEffortBroadcast.BestEffortBroadcast_Req_Message {
				Addresses: getMapKeys(module.Correct),
				Message: real_msg,
			}
			fmt.Printf("[GUARD] SENDING BROADCAST WITH MESSAGE: %s, TO: %v\n", real_msg, getMapKeys(module.Correct))
			module.BEB.Broadcast(msg)
		}
	}
}

func isSubset(correct, receivedfrom map[string]string) bool {
	if (len(correct) == 0) {
		return false
	}

	for key, value := range correct {
		if receivedValue, exists := receivedfrom[key]; !exists || receivedValue != value {
			return false
		}
	}
	return true
}

func (module *FloodingUniformConsensus) AppendProposed(proposed []string) {
	for _, value := range proposed {
		module.ProposalSet[value] = value
	}
}

func getKeys(myMap map[string]string) []string {
	keys := make([]string, 0, len(myMap))
	for key := range myMap {
		keys = append(keys, key)
	}
	return keys
}


func GetMin(stringsArray []string) string {
	minString := stringsArray[0]

	for _, str := range stringsArray[1:] {
		if str < minString {
			minString = str
		}
	}

	return minString
}

func concatenateValues(proposalSet map[string]string) string {
	var result strings.Builder

	// Loop through the values and concatenate with "§"
	first := true
	for _, value := range proposalSet {
		if !first {
			result.WriteString("§") // Add "§" as a separator
		}
		result.WriteString(value)
		first = false
	}

	return result.String()
}