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

package PerfectFailureDetector

import (
	"SD/PP2PLink"
	"fmt"
	"strings"
	"sync"
	"time"
)

type CrashEvent struct {
	Crashed_address string
	Message string
}

type PerfectFailureDetector struct {
	Crash chan CrashEvent
	Pp2plink *PP2PLink.PP2PLink
	Addresses []string
	Address string
	alive map[string]string
	detected map[string]string
	dbg bool
	timer     *time.Ticker
	timerLock sync.Mutex
}

func (module *PerfectFailureDetector) Init() {
	module.InitAlives();
}

func (module *PerfectFailureDetector) InitAlives() {
	for i := 0; i < len(module.Addresses); i++ {
		module.alive[module.Addresses[i]] = module.Addresses[i]
	}
}

func NewPFD(addresses []string, _dbg bool) *PerfectFailureDetector {
	fmt.Println("PFD!!!!")
	pfd := &PerfectFailureDetector{
		Crash: make(chan CrashEvent, 100),
		Pp2plink: PP2PLink.NewPP2PLink(addresses[0], _dbg),
		Addresses: addresses[1:],
		Address: addresses[0],
		alive:  make(map[string]string),
		detected: make(map[string]string),
		timer: nil,
		dbg: _dbg,
	}
	// pfd.outDbg("Init PDF!")
	pfd.Init()
	pfd.Start()
	return pfd
}

func NewPFDWithP2P(addresses []string, P2PLink * PP2PLink.PP2PLink, _dbg bool) *PerfectFailureDetector {
	pfd := &PerfectFailureDetector{
		Crash: make(chan CrashEvent),
		Pp2plink: P2PLink,
		Addresses: addresses,
		Address: addresses[0],
		alive:  make(map[string]string),
		detected: make(map[string]string),
		timer: nil,
		dbg: _dbg,
	}
	// pfd.outDbg("Init PDF!")
	pfd.Init()
	pfd.Start()
	return pfd
}

func (module *PerfectFailureDetector) Start() {
	go func() {
		module.timerLock.Lock()
		module.timer = time.NewTicker(10 * time.Second)
		module.timerLock.Unlock()

		defer module.StopTimer()
		for {
			select {
				case y := <- module.Pp2plink.Ind:
					module.HandleHeartbeatRequest(y)
				case <-module.timer.C:
					if (module.dbg) {
						fmt.Println("10 secs passed: "+module.Address)
					}
					module.HandleTimeout()
				}
			}
		}()
	}


func (module *PerfectFailureDetector) StopTimer() {
	module.timerLock.Lock()
	defer module.timerLock.Unlock()

	if module.timer != nil {
		module.timer.Stop()
		module.timer = nil
	}
}

func (module *PerfectFailureDetector) HandleTimeout () {
	for _, address := range module.Addresses {
		_, exists_alive := module.alive[address]
		_, exists_detected := module.detected[address]

		if (module.dbg) {
			fmt.Printf("Address %s is alive? %t\n", address, exists_alive)
			fmt.Printf("Address %s is detected? %t\n", address, exists_detected)
		}

		if !exists_alive && !exists_detected {
			module.detected[address] = address
			if (module.dbg) {
				fmt.Printf("************ Node %s HAS CRASHED!!!!! *************\n", address)
			}
			module.Crash <- CrashEvent {
				Crashed_address: address,
				Message: "Node" + address + "Crashed!",
			}
		}

		if (module.dbg) {
			fmt.Printf("Im %s and Im sending a message of type Request to: %s\n", module.Address, address)
		}

		module.Pp2plink.Send(PP2PLink.PP2PLink_Req_Message {
			Message:  module.Address + "§" +  address + "§Request",
			To: address,
		})
	}

	module.alive = make(map[string]string)
	//star timer?
}

func (module *PerfectFailureDetector) HandleHeartbeatRequest(request PP2PLink.PP2PLink_Ind_Message) {
	msg_split := strings.Split(request.Message, "§")
	msg_type := msg_split[2]
	from, to := msg_split[0], msg_split[1]

	if (msg_type != "Request" && msg_type != "Reply") {
		if (module.dbg) {
			fmt.Printf("[PFD HEARTBEAT] Just got a message %s, from: %s, to: %s, RETRANSMITING....\n", request.Message, from, to)
		}
		module.Pp2plink.Req <- 
			PP2PLink.PP2PLink_Req_Message{
				To: to,
				Message: request.Message,
			}
			
		return
	}
	if msg_type == "Request" {
		module.Pp2plink.Send(PP2PLink.PP2PLink_Req_Message {
			Message: module.Address + "§" + from + "§Reply",
			To: from,
		})
	} else {
		module.alive[from] = from
	}
	
}