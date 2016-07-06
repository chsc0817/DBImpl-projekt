package csgo

import "fmt"

var GoroutineFinished []bool
//this function splits the rows of a Relation
//aggregates each one seperatly then combines them and aggregates them again 
func (rl *Relation) Aggregate(aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	var RowLength int
	FirstHalf := make([]Column,0)
	SecondHalf := make([]Column,0)
	switch rl.Columns[0].Signature.Type {
	case INT:
		RowLength = len(rl.Columns[0].Data.([]int))
	case FLOAT:
		RowLength = len(rl.Columns[0].Data.([]float64))
	case STRING:
		RowLength = len(rl.Columns[0].Data.([]string))
	}	
	//split the rows in half
	for i:=0;i<len(rl.Columns);i++{
		FirstHalf = append(FirstHalf, rl.Columns[i])
		FirstHalf[i].Signature.Type = rl.Columns[i].Signature.Type 
		SecondHalf = append(SecondHalf, rl.Columns[i])
		SecondHalf[i].Signature.Type = rl.Columns[i].Signature.Type 
		switch rl.Columns[i].Signature.Type{			
			case INT:
				FirstHalf[i].Data = rl.Columns[i].Data.([]int)[0:RowLength/2] 	
				SecondHalf[i].Data = rl.Columns[i].Data.([]int)[RowLength/2:] 	
			case FLOAT:
				FirstHalf[i].Data = rl.Columns[i].Data.([]float64)[0:RowLength/2] 	
				SecondHalf[i].Data = rl.Columns[i].Data.([]float64)[RowLength/2:]  
			case STRING:
				FirstHalf[i].Data = rl.Columns[i].Data.([]string)[0:RowLength/2] 	
				SecondHalf[i].Data = rl.Columns[i].Data.([]string)[RowLength/2:] 
	}	}	
	GoroutineFinished = append(GoroutineFinished,false,false)
	//aggregate each half
	go agg(aggregate, aggrFunc, FirstHalf, false, 0)
	go agg(aggregate, aggrFunc, SecondHalf, false, 1)
	//wait until both functions finished
	for FinishedGoRoutines := 0; FinishedGoRoutines < 2;{
		if GoroutineFinished[FinishedGoRoutines] {FinishedGoRoutines++}}
	//combine both aggregations...
	for i:=0;i<len(rl.Columns);i++{
		switch FirstHalf[i].Signature.Type{			
			case INT:
				FirstHalf[i].Data = append(FirstHalf[i].Data.([]int),SecondHalf[i].Data.([]int)...) 				
			case FLOAT:
				FirstHalf[i].Data = append(FirstHalf[i].Data.([]float64),SecondHalf[i].Data.([]float64)...)				
			case STRING:
				FirstHalf[i].Data = append(FirstHalf[i].Data.([]string),SecondHalf[i].Data.([]string)...) 				
	}	}
	//...and aggregate them
	agg(aggregate,aggrFunc,FirstHalf,true, 1)
	for i:=0;i<len(rl.Columns);i++{
	rl.Columns[i].Data = FirstHalf[i].Data
	}	
	rl.Columns[len(rl.Columns)-1].Signature.Name = "Aggregate"
	if aggrFunc == COUNT {rl.Columns[len(rl.Columns)-1].Signature.Type = INT}
	return rl	
}


//the actual aggregate function
//if you combine already aggregated Rows set alreadyGrouped true
//GoroutineNumber is used to synchronize all Goroutines. Only if all Goroutines have finished the main program will continue 
func agg(aggregate AttrInfo, aggrFunc AggrFunc,aggrColumn []Column, alreadyGrouped bool, GoroutineNumber int) {
	var aggrPos int
	var RowLength int
	ColumnLength := len(aggrColumn)
	HasSameValues := false
	if !alreadyGrouped {
		if aggregate.Type == STRING && aggrFunc != COUNT {
			fmt.Println("Only COUNT for string supported")		
		}	
		//find the aggregate Column
		for i := 0; i < ColumnLength; i++ {
			if aggrColumn[i].Signature == aggregate {
				aggrPos = i
				break
		}	}
		//place aggregate Column at the end
		if aggrPos != ColumnLength - 1 {
			temp := aggrColumn[aggrPos]
			aggrColumn[aggrPos] = aggrColumn[ColumnLength -1] 
			aggrColumn[ColumnLength -1] = temp	
			aggrPos = ColumnLength - 1		
		}
		//COUNT replaces the aggregate Column with an empty int list if it is not already an int list
		if aggrFunc == COUNT && aggregate.Type == FLOAT || aggregate.Type == STRING {
			aggrColumn[aggrPos].Data = make([]int, 0)	
		}
	} else {
		aggrPos = ColumnLength - 1
	}
	//get row length
	switch aggrColumn[0].Signature.Type {
	case INT:
		RowLength = len(aggrColumn[0].Data.([]int))
	case FLOAT:
		RowLength = len(aggrColumn[0].Data.([]float64))
	case STRING:
		RowLength = len(aggrColumn[0].Data.([]string))
	}		
	AlreadyAggregatedRows := 0
	//the main loop
	for i:=0; i < RowLength; i++ {
		for  RowIndex:=0; RowIndex < AlreadyAggregatedRows; RowIndex++ {	
			HasSameValues = true
			//check if the current viewed row matches an already aggregated row 
			for CurrentColumn :=0; CurrentColumn < aggrPos; CurrentColumn++ {				
				switch aggrColumn[CurrentColumn].Signature.Type {
					case INT:
						if aggrColumn[CurrentColumn].Data.([]int)[RowIndex] != aggrColumn[CurrentColumn].Data.([]int)[i] {							
							HasSameValues = false		
						}	
					case FLOAT:
						if aggrColumn[CurrentColumn].Data.([]float64)[RowIndex] != aggrColumn[CurrentColumn].Data.([]float64)[i] {							
							HasSameValues = false		
						}
					case STRING:
						if aggrColumn[CurrentColumn].Data.([]string)[RowIndex] != aggrColumn[CurrentColumn].Data.([]string)[i] {							
							HasSameValues = false	
				}		}
				if HasSameValues == false {
					break
			}	}
			//current row matches, change aggregate Column if necessary
			if HasSameValues {
				switch aggrFunc {
					case COUNT:
						aggrColumn[aggrPos].Data.([]int)[RowIndex] = aggrColumn[aggrPos].Data.([]int)[RowIndex] + 1 					
					case MAX:
						switch aggregate.Type {
							case INT:
								if aggrColumn[aggrPos].Data.([]int)[RowIndex] < aggrColumn[aggrPos].Data.([]int)[i] {
									aggrColumn[aggrPos].Data.([]int)[RowIndex] = aggrColumn[aggrPos].Data.([]int)[i] }
							case FLOAT:
								if aggrColumn[aggrPos].Data.([]float64)[RowIndex] < aggrColumn[aggrPos].Data.([]float64)[i] {
									aggrColumn[aggrPos].Data.([]float64)[RowIndex] = aggrColumn[aggrPos].Data.([]float64)[i] }					
						}
					case MIN:
						switch aggregate.Type {
							case INT:
								if aggrColumn[aggrPos].Data.([]int)[RowIndex] > aggrColumn[aggrPos].Data.([]int)[i] {
									aggrColumn[aggrPos].Data.([]int)[RowIndex] = aggrColumn[aggrPos].Data.([]int)[i] }
							case FLOAT:
								if aggrColumn[aggrPos].Data.([]float64)[RowIndex] > aggrColumn[aggrPos].Data.([]float64)[i] {
									aggrColumn[aggrPos].Data.([]float64)[RowIndex] = aggrColumn[aggrPos].Data.([]float64)[i] }					
						}
					case SUM:
						switch aggregate.Type {
							case INT:
								aggrColumn[aggrPos].Data.([]int)[RowIndex] = aggrColumn[aggrPos].Data.([]int)[RowIndex] + aggrColumn[aggrPos].Data.([]int)[i]
							case FLOAT:
								aggrColumn[aggrPos].Data.([]float64)[RowIndex] = aggrColumn[aggrPos].Data.([]float64)[RowIndex] + aggrColumn[aggrPos].Data.([]float64)[i] }					
						}	
				break
		}	}
		//no match for current row found, add it to aggregated rows and replace aggregate column value 
		if !HasSameValues {
			for j:=0;j < aggrPos; j++ {
				switch aggrColumn[j].Signature.Type{
					case INT:
						aggrColumn[j].Data.([]int)[AlreadyAggregatedRows] = aggrColumn[j].Data.([]int)[i] 
					case FLOAT:
						aggrColumn[j].Data.([]float64)[AlreadyAggregatedRows] = aggrColumn[j].Data.([]float64)[i] 
					case STRING:
						aggrColumn[j].Data.([]string)[AlreadyAggregatedRows] = aggrColumn[j].Data.([]string)[i] 
			}	}
			if aggrFunc == COUNT && !alreadyGrouped {
				switch aggrColumn[aggrPos].Signature.Type {
					case INT:
					aggrColumn[aggrPos].Data.([]int)[AlreadyAggregatedRows] = 1
					default:
					aggrColumn[aggrPos].Data = append(aggrColumn[aggrPos].Data.([]int), 1)
				}				
			} else {
				switch aggrColumn[aggrPos].Signature.Type {
					case INT:
						aggrColumn[aggrPos].Data.([]int)[AlreadyAggregatedRows] = aggrColumn[aggrPos].Data.([]int)[i] 						
					case FLOAT:
						aggrColumn[aggrPos].Data.([]float64)[AlreadyAggregatedRows] = aggrColumn[aggrPos].Data.([]float64)[i] 								
			}	}	
			AlreadyAggregatedRows++
	}	}
	//all rows tested, change aggregate type if necessary and cut off all rows after the last aggregated one
	if aggrFunc == COUNT && aggregate.Type == FLOAT || aggregate.Type == STRING {
		aggrColumn[aggrPos].Signature.Type = INT	
	}
	for j:=0; j < ColumnLength; j++ {
		switch aggrColumn[j].Signature.Type{
			case INT:
				aggrColumn[j].Data = aggrColumn[j].Data.([]int)[0:AlreadyAggregatedRows] 	
			case FLOAT:
				aggrColumn[j].Data = aggrColumn[j].Data.([]float64)[0:AlreadyAggregatedRows] 
			case STRING:
				aggrColumn[j].Data = aggrColumn[j].Data.([]string)[0:AlreadyAggregatedRows] 
	}	}	
	//notify the main program that this goroutine has finished
	GoroutineFinished[GoroutineNumber] = true
}