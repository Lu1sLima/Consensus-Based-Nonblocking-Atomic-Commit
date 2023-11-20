/*
Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
Professor: Fernando Dotti  (https://fldotti.github.io/)
Modulo representando Berst Effort Broadcast tal como definido em:

	Introduction to Reliable and Secure Distributed Programming
	Christian Cachin, Rachid Gerraoui, Luis Rodrigues

* Semestre 2018/2 - Primeira versao.  Estudantes:  Andre Antonitsch e Rafael Copstein
Para uso vide ao final do arquivo, ou aplicacao chat.go que usa este
*/
package BestEffortBroadcast

import (
	"fmt"
	"strings"

	PP2PLink "SD/PP2PLink"
)

type BestEffortBroadcast_Req_Message struct {
	Addresses []string
	Message   string
}

type BestEffortBroadcast_Ind_Message struct {
	From    string
	Message string
}

type BestEffortBroadcast_Module struct {
	Ind      chan BestEffortBroadcast_Ind_Message
	Req      chan BestEffortBroadcast_Req_Message
	Address string
	Pp2plink *PP2PLink.PP2PLink
	dbg      bool
}

func (module *BestEffortBroadcast_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . [ BEB msg : " + s + " ]")
	}
}

func (module *BestEffortBroadcast_Module) Init(address string) {
	module.InitD(address, true)
}

func (module *BestEffortBroadcast_Module) InitD(address string, _dbg bool) {
	module.dbg = _dbg
	module.outDbg("Init BEB!")
	module.Pp2plink = PP2PLink.NewPP2PLink(address, _dbg)
	module.Start()
}

func NewBEB(address string, _dbg bool) *BestEffortBroadcast_Module {
	fmt.Println(address)
	beb := &BestEffortBroadcast_Module{
		Req: make(chan BestEffortBroadcast_Req_Message),
		Ind: make(chan BestEffortBroadcast_Ind_Message),
		Pp2plink: PP2PLink.NewPP2PLink(address, _dbg),
		dbg: _dbg,
	}
	beb.outDbg("Init BEB!")
	beb.Start()
	return beb
}

func NewBEBWithP2P(address string, P2PLink *PP2PLink.PP2PLink, _dbg bool) *BestEffortBroadcast_Module {
	beb := &BestEffortBroadcast_Module{
		Req: make(chan BestEffortBroadcast_Req_Message, 100),
		Ind: make(chan BestEffortBroadcast_Ind_Message, 100),
		Pp2plink: P2PLink,
		Address: address,
		dbg: _dbg,
	}
	beb.outDbg("Init BEB!")
	beb.Start()
	return beb
}

func (module *BestEffortBroadcast_Module) Start() {

	go func() {
		for {
			select {
			case y := <-module.Req:
				module.Broadcast(y)
			case y := <-module.Pp2plink.Ind:
				module.Deliver(PP2PLink2BEB(y))
			}
		}
	}()
}

func (module *BestEffortBroadcast_Module) Broadcast(message BestEffortBroadcast_Req_Message) {

	// aqui acontece o envio um opara um, para cada processo destinatario
	// em caso de injecao de falha no originador, no meio de um broadcast
	// este loop deve ser interrompido, tendo a mensagem ido para alguns mas nao para todos processos

	for i := 0; i < len(message.Addresses); i++ {
		msg := BEB2PP2PLink(message)
		msg.To = message.Addresses[i]
		msg.Message = module.Address + "§" + message.Addresses[i] + "§" + message.Message
		module.Pp2plink.Req <- msg
		module.outDbg("Sent to " + message.Addresses[i])
	}
}

func (module *BestEffortBroadcast_Module) Deliver(message BestEffortBroadcast_Ind_Message) {

	// fmt.Println("Received '" + message.Message + "' from " + message.From)
	if (strings.Contains(message.Message, "Request") || strings.Contains(message.Message, "Reply")) {
		msg_split := strings.Split(message.Message, "§")
		_, to := msg_split[0], msg_split[1]
		module.Pp2plink.Send( // tá pegando as msgs do PFD, entao eu só reenvio qnd é ele.
			PP2PLink.PP2PLink_Req_Message{
				Message: message.Message,
				To: to,
			})
		return 
	}
	module.Ind <- message
	// fmt.Println("# End BEB Received")
}

func BEB2PP2PLink(message BestEffortBroadcast_Req_Message) PP2PLink.PP2PLink_Req_Message {

	return PP2PLink.PP2PLink_Req_Message{
		To:      message.Addresses[0],
		Message: message.Message}

}

func PP2PLink2BEB(message PP2PLink.PP2PLink_Ind_Message) BestEffortBroadcast_Ind_Message {

	return BestEffortBroadcast_Ind_Message{
		From:    message.From,
		Message: message.Message}
}