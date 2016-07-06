package csgo

var GoroutineFinished []bool


func (rl *Relation) Aggregate(aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	var RowLength int
	c1 := make(chan []Column) 
	c2 := make(chan []Column)	
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
	go agg(aggregate, aggrFunc, FirstHalf, c1, false, 0)
	go agg(aggregate, aggrFunc, SecondHalf, c2, false, 1)	
	//wait until both functions finished
	for FinishedGoRoutines := 0; FinishedGoRoutines < 2;{
		if GoroutineFinished[FinishedGoRoutines] {FinishedGoRoutines++}}
	//combine both aggregations...	
	FirstHalf = <- c1
	SecondHalf = <- c2
	for i:=0;i<len(FirstHalf);i++{
		switch FirstHalf[i].Signature.Type{			
			case INT:
				FirstHalf[i].Data = append(FirstHalf[i].Data.([]int),SecondHalf[i].Data.([]int)...) 				
			case FLOAT:
				FirstHalf[i].Data = append(FirstHalf[i].Data.([]float64),SecondHalf[i].Data.([]float64)...)				
			case STRING:
				FirstHalf[i].Data = append(FirstHalf[i].Data.([]string),SecondHalf[i].Data.([]string)...) 				
	}	}
	//...and aggregate them
	aggregate.Type=INT
	GoroutineFinished[0]=false
	go agg(aggregate,aggrFunc,FirstHalf,c1, true, 0)
	for {if GoroutineFinished[0]==true {break}}
	rl.Columns = <-c1
	rl.Columns[len(rl.Columns)-1].Signature.Name = "Aggregate"
	if aggrFunc == COUNT {rl.Columns[len(rl.Columns)-1].Signature.Type = INT}
	return rl	
}


func agg(aggregate AttrInfo, aggrFunc AggrFunc,dataset []Column, c chan []Column, alreadyGrouped bool, GoroutineNumber int) {
	if aggregate.Type == STRING && aggrFunc != COUNT {
		panic("Only COUNT for string supported")		
	}
	var aggrColumn Column
	var emptyColumn Column
	currentRow := make([]interface{}, 0)
	JoinColumns := make([]Column, 0)
	groupColumns := make([]Column, 0)
	groupSig := make([]AttrInfo, 0)
	//divide rl in groupColumns and aggrColumn
	for i := 0; i < len(dataset); i++ {

		if dataset[i].Signature != aggregate {

			groupColumns = append(groupColumns, dataset[i])
			groupSig = append(groupSig, dataset[i].Signature)
		} else {
			aggrColumn = dataset[i]

	}	}
	//initialize join columns with groupColumn...
	for i := 0; i < len(groupSig); i++ {
		JoinColumns = append(JoinColumns, emptyColumn)
		JoinColumns[i].Signature = groupSig[i]
		switch JoinColumns[i].Signature.Type {
		case INT:
			JoinColumns[i].Data = make([]int, 0)
		case FLOAT:
			JoinColumns[i].Data = make([]float64, 0)
		case STRING:
			JoinColumns[i].Data = make([]string, 0)
	}	} 
	aggrPos := len(JoinColumns)
	JoinColumns = append(JoinColumns, emptyColumn)	
	JoinColumns[aggrPos].Signature = aggregate

	if aggrFunc == COUNT || aggregate.Type == INT {
		JoinColumns[aggrPos].Signature.Type = INT
		JoinColumns[aggrPos].Data = make([]int, 0)	
	} else {
		JoinColumns[aggrPos].Signature.Type = FLOAT
		JoinColumns[aggrPos].Data = make([]float64, 0)	
	}
	var aggrColumnLength int
	switch aggregate.Type {
	case INT:
		aggrColumnLength = len(aggrColumn.Data.([]int))
	case FLOAT:
		aggrColumnLength = len(aggrColumn.Data.([]float64))
	case STRING:
		aggrColumnLength = len(aggrColumn.Data.([]string))
	}	
	//add each Row to JoinColumns
	for i := 0; i < aggrColumnLength; i++ {
		currentRow = make([]interface{}, 0)
		for j := 0; j < len(groupColumns); j++ {
			switch groupColumns[j].Signature.Type {
				
			case INT:
				currentRow = append(currentRow, groupColumns[j].Data.([]int)[i])
			case FLOAT:
				currentRow = append(currentRow, groupColumns[j].Data.([]float64)[i])
			case STRING:
				currentRow = append(currentRow, groupColumns[j].Data.([]string)[i])
		}	}		
		JoinColumnsLength := 0
		if len(JoinColumns) > 0 {
			switch JoinColumns[0].Signature.Type {
			case INT:
				JoinColumnsLength = len(JoinColumns[0].Data.([]int))
			case FLOAT:
				JoinColumnsLength = len(JoinColumns[0].Data.([]float64))
			case STRING:
				JoinColumnsLength = len(JoinColumns[0].Data.([]string))
		}	}
		PosAggrColumns := -1
		//check if currentColumn already exists within JoinColumn
		for posInJoin := 0; posInJoin < JoinColumnsLength; posInJoin++ {		
			PosAggrColumns = -1
			for j := 0; j < len(currentRow); j++ {
				if PosAggrColumns == -2 {
					break
				}				
				switch JoinColumns[j].Signature.Type {
				case INT:
					if JoinColumns[j].Data.([]int)[posInJoin] != currentRow[j] {
						PosAggrColumns = -2
					}
				case FLOAT:
					if JoinColumns[j].Data.([]float64)[posInJoin] != currentRow[j] {
						PosAggrColumns = -2
					}
				case STRING:
					if JoinColumns[j].Data.([]string)[posInJoin] != currentRow[j] {
						PosAggrColumns = -2
			}	}	}
			//currentRow already in JoinColumns
			if PosAggrColumns == -1 {
				PosAggrColumns = posInJoin
				break
		}	}		
		if PosAggrColumns == -2||PosAggrColumns == -1 {
			//not in JoinColumns, add currentRow to JoinColumns

			if aggrFunc == COUNT && !alreadyGrouped {			
				currentRow = append(currentRow, 1)
			}else{
				if aggregate.Type == FLOAT {
					currentRow = append(currentRow, aggrColumn.Data.([]float64)[i])
				} else {
					currentRow = append(currentRow, aggrColumn.Data.([]int)[i])
			}	}		
			for j := 0; j < len(currentRow); j++ {	
				switch JoinColumns[j].Signature.Type {
				case INT:
					JoinColumns[j].Data = append(JoinColumns[j].Data.([]int), currentRow[j].(int))
				case FLOAT:
					JoinColumns[j].Data = append(JoinColumns[j].Data.([]float64), currentRow[j].(float64))
				case STRING:
					JoinColumns[j].Data = append(JoinColumns[j].Data.([]string), currentRow[j].(string))
			}	}
		} else {
			//already in JoinColumns, update aggr value
			switch aggregate.Type {
			case INT:				
				switch aggrFunc {
				case COUNT:
					if !alreadyGrouped {
						JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] + 1
					}else{
						JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] + aggrColumn.Data.([]int)[i]
					}					
				case SUM:
					JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] + aggrColumn.Data.([]int)[i]
				case MAX:
					if JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] < aggrColumn.Data.([]int)[i] {							
						JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]  = aggrColumn.Data.([]int)[i]
					}
				case MIN:
					if JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]  > aggrColumn.Data.([]int)[i] {
						JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]  = aggrColumn.Data.([]int)[i]
				}	}			
			case FLOAT:				
				switch aggrFunc {
				case COUNT:					
					JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] + 1
				case SUM:
					JoinColumns[aggrPos].Data.([]float64)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]float64)[PosAggrColumns] + aggrColumn.Data.([]float64)[i]
				case MAX:				
					if JoinColumns[aggrPos].Data.([]float64)[PosAggrColumns] < aggrColumn.Data.([]float64)[i] {							
						JoinColumns[aggrPos].Data.([]float64)[PosAggrColumns]  = aggrColumn.Data.([]float64)[i]
					}
				case MIN:
					if JoinColumns[aggrPos].Data.([]float64)[PosAggrColumns]  > aggrColumn.Data.([]float64)[i] {
						JoinColumns[aggrPos].Data.([]float64)[PosAggrColumns]  = aggrColumn.Data.([]float64)[i]
				}	}		
			case STRING:
				value := JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]
				JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = value + 1
	}	}	}
	GoroutineFinished[GoroutineNumber] = true
	c <- JoinColumns
}