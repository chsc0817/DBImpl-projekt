package csgo

import "fmt"

func (rl *Relation) Aggregate(aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	var aggrPos int
	var RowLength int
	ColumnLength := len(rl.Columns)
	HasSameValues := false
	
	if aggregate.Type == STRING && aggrFunc != COUNT {
		fmt.Println("Only COUNT for string supported")
		return rl
	}	
	//find the aggregate Column
	for i := 0; i < ColumnLength; i++ {
		if rl.Columns[i].Signature == aggregate {
			aggrPos = i
			break
	}	}
	//get row length and change AttrInfo for aggregate if necessary
	switch aggregate.Type {
	case INT:
		RowLength = len(rl.Columns[aggrPos].Data.([]int))
	case FLOAT:
		RowLength = len(rl.Columns[aggrPos].Data.([]float64))
	case STRING:
		RowLength = len(rl.Columns[aggrPos].Data.([]string))
		aggregate.Type = INT		
	}		
	//place aggregate Column at the end	
	if aggrPos != ColumnLength - 1 {
		temp := rl.Columns[aggrPos]
		rl.Columns[aggrPos] = rl.Columns[ColumnLength -1] 
		rl.Columns[ColumnLength -1] = temp	
		aggrPos = ColumnLength - 1		
	}
	rl.Columns[aggrPos].Signature.Name = "Aggregate"
	AlreadyAggregatedRows := 0

	for i:=0; i < RowLength; i++ {
		for  RowIndex:=0; RowIndex < AlreadyAggregatedRows; RowIndex++ {	
			HasSameValues = true
			for CurrentColumn :=0; CurrentColumn < aggrPos; CurrentColumn++ {				
				switch rl.Columns[CurrentColumn].Signature.Type {
					case INT:
						if rl.Columns[CurrentColumn].Data.([]int)[RowIndex] != rl.Columns[CurrentColumn].Data.([]int)[i] {							
							HasSameValues = false		
						}	
					case FLOAT:
						if rl.Columns[CurrentColumn].Data.([]float64)[RowIndex] != rl.Columns[CurrentColumn].Data.([]float64)[i] {							
							HasSameValues = false		
						}
					case STRING:
						if rl.Columns[CurrentColumn].Data.([]string)[RowIndex] != rl.Columns[CurrentColumn].Data.([]string)[i] {							
							HasSameValues = false	
				}		}
				if HasSameValues == false {
					break
			}	}
			if HasSameValues {
				switch aggrFunc {
					case COUNT:
						switch aggregate.Type{
							case INT:
								rl.Columns[aggrPos].Data.([]int)[RowIndex] = rl.Columns[aggrPos].Data.([]int)[RowIndex] + 1 
							case FLOAT:
								rl.Columns[aggrPos].Data.([]float64)[RowIndex] = rl.Columns[aggrPos].Data.([]float64)[RowIndex] + 1 		
						}
						
					case MAX:
						switch aggregate.Type {
							case INT:
								if rl.Columns[aggrPos].Data.([]int)[RowIndex] < rl.Columns[aggrPos].Data.([]int)[i] {
									rl.Columns[aggrPos].Data.([]int)[RowIndex] = rl.Columns[aggrPos].Data.([]int)[i] }
							case FLOAT:
								if rl.Columns[aggrPos].Data.([]float64)[RowIndex] < rl.Columns[aggrPos].Data.([]float64)[i] {
									rl.Columns[aggrPos].Data.([]float64)[RowIndex] = rl.Columns[aggrPos].Data.([]float64)[i] }					
						}
					case MIN:
						switch aggregate.Type {
							case INT:
								if rl.Columns[aggrPos].Data.([]int)[RowIndex] > rl.Columns[aggrPos].Data.([]int)[i] {
									rl.Columns[aggrPos].Data.([]int)[RowIndex] = rl.Columns[aggrPos].Data.([]int)[i] }
							case FLOAT:
								if rl.Columns[aggrPos].Data.([]float64)[RowIndex] > rl.Columns[aggrPos].Data.([]float64)[i] {
									rl.Columns[aggrPos].Data.([]float64)[RowIndex] = rl.Columns[aggrPos].Data.([]float64)[i] }					
						}
					case SUM:
						switch aggregate.Type {
							case INT:
								rl.Columns[aggrPos].Data.([]int)[RowIndex] = rl.Columns[aggrPos].Data.([]int)[RowIndex] + rl.Columns[aggrPos].Data.([]int)[i]
							case FLOAT:
								rl.Columns[aggrPos].Data.([]float64)[RowIndex] = rl.Columns[aggrPos].Data.([]float64)[RowIndex] + rl.Columns[aggrPos].Data.([]float64)[i] }					
						}	
				break
		}	}

		if !HasSameValues {
			for j:=0;j < aggrPos; j++ {
				switch rl.Columns[j].Signature.Type{
					case INT:
						rl.Columns[j].Data.([]int)[AlreadyAggregatedRows] = rl.Columns[j].Data.([]int)[i] 
					case FLOAT:
						rl.Columns[j].Data.([]float64)[AlreadyAggregatedRows] = rl.Columns[j].Data.([]float64)[i] 
					case STRING:
						rl.Columns[j].Data.([]string)[AlreadyAggregatedRows] = rl.Columns[j].Data.([]string)[i] 
			}	}

			if aggrFunc == COUNT{
				switch aggregate.Type {
					case INT:
					rl.Columns[aggrPos].Data.([]int)[AlreadyAggregatedRows] = 1
					case FLOAT:
					rl.Columns[aggrPos].Data.([]float64)[AlreadyAggregatedRows] = 1
				}				
			} else {
				switch aggregate.Type {
					case INT:
						rl.Columns[aggrPos].Data.([]int)[AlreadyAggregatedRows] = rl.Columns[aggrPos].Data.([]int)[i] 
						AlreadyAggregatedRows++
					case FLOAT:
						rl.Columns[aggrPos].Data.([]float64)[AlreadyAggregatedRows] = rl.Columns[aggrPos].Data.([]float64)[i] 								
			}	}	
			AlreadyAggregatedRows++
	}	}
	for j:=0; j < ColumnLength; j++ {
		switch rl.Columns[j].Signature.Type{
			case INT:
				rl.Columns[j].Data = rl.Columns[j].Data.([]int)[0:AlreadyAggregatedRows] 	
			case FLOAT:
				rl.Columns[j].Data = rl.Columns[j].Data.([]float64)[0:AlreadyAggregatedRows] 
			case STRING:
				rl.Columns[j].Data = rl.Columns[j].Data.([]string)[0:AlreadyAggregatedRows] 
	}	}	
	return rl	
}