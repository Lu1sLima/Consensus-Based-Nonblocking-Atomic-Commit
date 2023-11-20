// Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
//  Professor: Fernando Dotti  (https://fldotti.github.io/)

/*
LANCAR N PROCESSOS EM SHELL's DIFERENTES, UMA PARA CADA PROCESSO, O SEU PROPRIO ENDERECO EE O PRIMEIRO DA LISTA
go run chatCOB.go 127.0.0.1:5001  127.0.0.1:6001  127.0.0.1:7001   // o processo na porta 5001
go run chatCOB.go 127.0.0.1:6001  127.0.0.1:5001  127.0.0.1:7001   // o processo na porta 6001
go run chatCOB.go 127.0.0.1:7001  127.0.0.1:6001  127.0.0.1:5001     // o processo na porta ...
*/

package main

import (
	"bufio"
	"fmt"
	"os"

	. "SD/UC"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Please specify at least one address:port!")
		fmt.Println("go run chatConsensus.go 127.0.0.1:5001  127.0.0.1:6001")
		fmt.Println("go run chatConsensus.go 127.0.0.1:6001  127.0.0.1:5001")
		return
	}

	addresses := os.Args[1:]

	fmt.Println(addresses)
	
	FUC := NewFUC(addresses, true)

	// enviador de broadcasts
	go func() {

		scanner := bufio.NewScanner(os.Stdin)
		var msg string

		for {
			if scanner.Scan() {
				msg = scanner.Text()
				propose := ProposeEvent{
					To: addresses[0],
					Message:  msg}
				FUC.Propose <- propose
			}

		}
	}()

	decided := <- FUC.Decide
	fmt.Printf("Valor decidido: %s\n", decided) 
	
	// // receptor de broadcasts
	// go func() {
	// 	for {
	// 		in := <-cob.Ind // RECEBE MENSAGEM DE QUALQUER PROCESSO
	// 		message := strings.Split(in.Message, "§")
	// 		in.From = message[1]
	// 		registro = append(registro, in.Message)
	// 		in.Message = message[0]
			
	// 		// imprime a mensagem recebida na tela
	// 		// fmt.Printf("               Message from %v: %v\n", in.From, in.Message)
	// 	}
	// }()

	blq := make(chan int)
	<-blq
}
