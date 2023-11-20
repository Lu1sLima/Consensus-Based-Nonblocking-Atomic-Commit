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
	"fmt"
	"os"

	. "SD/PFD"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Please specify at least one address:port!")
		fmt.Println("go run chatPFD.go 0 127.0.0.1:5001  127.0.0.1:6001")
		fmt.Println("go run chatPFD.go 1 127.0.0.1:6001  127.0.0.1:5001")
		return
	}

	idx := os.Args[1]
	addresses := os.Args[2:]
	
	fmt.Println("ID: "+idx)
	pfd := NewPFD(addresses, true)

	for {
		<- pfd.Crash
	}

	blq := make(chan int)
	<-blq
}
