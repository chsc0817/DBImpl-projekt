package csgo

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

//Creating a ColumnStore for test purposes
func NewColumnStore() ColumnStorer {
	var cs ColumnStore
	cs.Relations = make(map[string]Relationer)
	return &cs
}

//Create a new Relation
//Create the number of Columns in the Relation
//Register the AttrInfo in the Columns
func (cs *ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	var rel Relation

	rel.Name = tabName
	//Create the number of Columns
	rel.Columns = make([]Column, len(sig))
	//Register the AttrInfo in the Columns
	for i := 0; i < len(sig); i++ {
		rel.Columns[i].Signature = sig[i]
	}
	return &rel
}

//Returns the pointer on the Relation
func (cs *ColumnStore) GetRelation(relName string) Relationer {
	return cs.Relations[relName]
}

//Load a csv or tbl file and add the data into your Column Store
//File hat not the same number of Columns than the Relation.
//Create the data slices and insert the first row
//Reading in the data by their type
//Following Errors will stop the program:
//Error if file could not be opened or found
//Error if inputType is not ColumnType
//Error if file could not be read
func (rl *Relation) Load(csvFile string, separator rune) {
	file, err := os.Open(csvFile)

	//Error if file could not be opened or found
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	reader := csv.NewReader(file)
	reader.Comma = separator
	record, err := reader.Read()

	//File hat not the same number of Columns than the Relation.
	if len(record) != len(rl.Columns) {
		fmt.Println("Number of defined columns is different than the number of columns in ", path.Base(csvFile))
		os.Exit(1)
	}
	//Create the data slices and insert the first row
	for i := 0; i < len(record); i++ {
		switch rl.Columns[i].Signature.Type {
		case INT:
			rl.Columns[i].Data = make([]int, 0)
			datas, err := strconv.Atoi(record[i])

			//Error if inputType is not int
			if err != nil {
				fmt.Println("error while loading", path.Base(csvFile))
				fmt.Print("row ", len(rl.Columns[i].Data.([]int))+1, ", column ", i+1, ": ")
				fmt.Print("\"", record[i], "\"")
				fmt.Println(" is not type int")
				os.Exit(1)
			}
			rl.Columns[i].Data = append(rl.Columns[i].Data.([]int), datas)

		case FLOAT:
			rl.Columns[i].Data = make([]float64, 0)
			datas, err := strconv.ParseFloat(record[i], 64)

			//Error if inputType is not float
			if err != nil {
				fmt.Println("error while loading", path.Base(csvFile))
				fmt.Print("row ", len(rl.Columns[i].Data.([]int))+1, ", column ", i+1, ": ")
				fmt.Print("\"", record[i], "\"")
				fmt.Println(" is not type float")
				os.Exit(1)
			}
			rl.Columns[i].Data = append(rl.Columns[i].Data.([]float64), datas)

		case STRING:
			rl.Columns[i].Data = make([]string, 0)
			rl.Columns[i].Data = append(rl.Columns[i].Data.([]string), record[i])
		}
	}

	for {
		record, err = reader.Read()

		//Breaks at the end of the file
		if err == io.EOF {
			break
		}

		//Error if file could not be read
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		for i := 0; i < len(record); i++ {
			//Reading in the data by their type
			switch rl.Columns[i].Signature.Type {
			case INT:
				datas, err := strconv.Atoi(record[i])

				//Error if inputType is not int
				if err != nil {
					fmt.Println("error while loading", path.Base(csvFile))
					fmt.Print("row ", len(rl.Columns[i].Data.([]int))+1, ", column ", i+1, ": ")
					fmt.Print("\"", record[i], "\"")
					fmt.Println(" is not type int")
					os.Exit(1)
				}
				rl.Columns[i].Data = append(rl.Columns[i].Data.([]int), datas)

			case FLOAT:
				datas, err := strconv.ParseFloat(record[i], 64)

				//Error if inputType is not float
				if err != nil {
					fmt.Println("error while loading", path.Base(csvFile))
					fmt.Print("row ", len(rl.Columns[i].Data.([]int))+1, ", column ", i+1, ": ")
					fmt.Print("\"", record[i], "\"")
					fmt.Println(" is not type float")
					os.Exit(1)
				}
				rl.Columns[i].Data = append(rl.Columns[i].Data.([]float64), datas)

			case STRING:
				rl.Columns[i].Data = append(rl.Columns[i].Data.([]string), record[i])
			}
		}
	}
}

//Test all Column if their AttrInfo is one of the wanted AttrInfo/Colums
//Returns a Relation where the Columns are filtered by their AttrInfo
func (rl *Relation) Scan(colList []AttrInfo) Relationer {
	var ret Relation
	ret.Name = rl.Name
	//Test all Column if their AttrInfo is one of the wanted AttrInfo/Colums
	for i := 0; i < len(colList); i++ {
		for j := 0; j < len(rl.Columns); j++ {
			if rl.Columns[j].Signature == colList[i] {
				ret.Columns = append(ret.Columns, rl.Columns[j])
			}
		}
	}
	return &Relation{ret.Name, ret.Columns}
}

//Determine the length of a slice in an interface
func interfacelen(inter interface{}) int {
	switch inter.(type) {
	case []int:
		return len(inter.([]int))
	case []float64:
		return len(inter.([]float64))
	case []string:
		return len(inter.([]string))
	}
	return 0
}

//Test if the Columns are declared and declare them if they're not
//Copys from the inputColumns into the outputColumns
func copyColumns(outputColumns []Column, inputColumns []Column, record int) []Column {
	//Test if the Columns are declared and declare them if they're not
	for j := 0; j < len(inputColumns); j++ {
		if nil == outputColumns[j].Data {
			switch inputColumns[j].Data.(type) {
			case []int:
				outputColumns[j].Data = make([]int, 0)
			case []float64:
				outputColumns[j].Data = make([]float64, 0)
			case []string:
				outputColumns[j].Data = make([]string, 0)
			}
		}
	}
	//Copys the inputColumns into the outputColumns
	for j := 0; j < len(inputColumns); j++ {
		switch inputColumns[j].Data.(type) {
		case []int:
			outputColumns[j].Data = append(outputColumns[j].Data.([]int), inputColumns[j].Data.([]int)[record])
		case []float64:
			outputColumns[j].Data = append(outputColumns[j].Data.([]float64), inputColumns[j].Data.([]float64)[record])
		case []string:
			outputColumns[j].Data = append(outputColumns[j].Data.([]string), inputColumns[j].Data.([]string)[record])
		}
	}
	return outputColumns
}

//Filter the Relation for records
//Create the new Relation + Columns and search the Column with which we shall compare
//Compare the data and the searched Value and put the right ones in the new Relation
//Strings are only compared on equality and  inequality
func (rl Relation) Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer {
	var colu int
	var ret Relation
	var create_column Column

	//Create the new Relation + Columns and search the Column with which we shall compare
	ret.Name = rl.Name
	//If the Relation has no Columns return
	if 0 == len(rl.Columns) {
		return &ret
	}
	for i := 0; i < len(rl.Columns); i++ {
		create_column.Signature = rl.Columns[i].Signature
		ret.Columns = append(ret.Columns, create_column)
		if rl.Columns[i].Signature == col {
			colu = i
		}
	}
	//Compare the data and the searched Value and put the right ones in the new Relation
	for i := 0; i < interfacelen(rl.Columns[0].Data); i++ {
		switch comp {
		case EQ:
			switch rl.Columns[colu].Signature.Type {
			case INT:
				if rl.Columns[colu].Data.([]int)[i] == compVal.(int) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case FLOAT:
				if rl.Columns[colu].Data.([]float64)[i] == compVal.(float64) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case STRING:
				if rl.Columns[colu].Data.([]string)[i] == compVal.(string) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			}
		case NEQ:
			switch rl.Columns[colu].Signature.Type {
			case INT:
				if rl.Columns[colu].Data.([]int)[i] != compVal.(int) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case FLOAT:
				if rl.Columns[colu].Data.([]float64)[i] != compVal.(float64) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case STRING:
				if rl.Columns[colu].Data.([]string)[i] != compVal.(string) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			}
		case LT:
			switch rl.Columns[colu].Signature.Type {
			case INT:
				if rl.Columns[colu].Data.([]int)[i] < compVal.(int) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case FLOAT:
				if rl.Columns[colu].Data.([]float64)[i] < compVal.(float64) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			}
		case LEQ:
			switch rl.Columns[colu].Signature.Type {
			case INT:
				if rl.Columns[colu].Data.([]int)[i] <= compVal.(int) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case FLOAT:
				if rl.Columns[colu].Data.([]float64)[i] <= compVal.(float64) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			}
		case GT:
			switch rl.Columns[colu].Signature.Type {
			case INT:
				if rl.Columns[colu].Data.([]int)[i] > compVal.(int) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case FLOAT:
				if rl.Columns[colu].Data.([]float64)[i] > compVal.(float64) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			}
		case GEQ:
			switch rl.Columns[colu].Signature.Type {
			case INT:
				if rl.Columns[colu].Data.([]int)[i] >= compVal.(int) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			case FLOAT:
				if rl.Columns[colu].Data.([]float64)[i] >= compVal.(float64) {
					ret.Columns = copyColumns(ret.Columns, rl.Columns, i)
				}
			}
		}
	}
	return &ret
}

//Converts the Interface to a String
func interfaceToString(inputInterface interface{}, j int) string {
	switch inputInterface.(type) {
	case []int:
		return strconv.Itoa(inputInterface.([]int)[j])
	case []float64:
		return strconv.FormatFloat(inputInterface.([]float64)[j], 'E', -1, 64)
	//string
	default:
		return inputInterface.([]string)[j]
	}
}

//Prints the relation
//Fetching  the data
//Print the column names
//Testing for the max width of the strings
//Print the datas in the columns
func (rl *Relation) Print() {
	var dataout = make([][]string, len(rl.Columns))
	var metaout string
	var width = make([]int, len(rl.Columns))
	var dataSetCount int
	var columnCount int

	//fmt.Println( rl.Name )
	//fmt.Println()
	columnCount = len(rl.Columns)
	//If the number of Columns 0 than return
	if 0 == columnCount {
		return
	}
	dataSetCount = interfacelen(rl.Columns[0].Data)
	//Fetching  the data
	for i := 0; i < len(rl.Columns); i++ {
		dataout[i] = append(dataout[i], rl.Columns[i].Signature.Name)
	}
	for j := 0; j < dataSetCount; j++ {
		for i := 0; i < columnCount; i++ {
			dataout[i] = append(dataout[i], interfaceToString(rl.Columns[i].Data, j))
		}
	}
	//Testing for the max width of the strings
	for i := 0; i < columnCount; i++ {
		width[i] = 0
		for j := 0; j < dataSetCount; j++ {
			if len(dataout[i][j]) > width[i] {
				width[i] = len(dataout[i][j])
			}
		}
	}
	//Print the column names
	metaout = "| "
	for i := 0; i < columnCount; i++ {
		for j := 0; j < (width[i] - len(dataout[i][0])); j++ {
			metaout = metaout + " "
		}
		metaout = metaout + dataout[i][0] + " | "
	}
	fmt.Println(metaout)
	for i := 0; i < len(metaout)-1; i++ {
		fmt.Print("-")
	}
	fmt.Println()
	//Print the datas in the columns
	for j := 1; j < dataSetCount+1; j++ {
		fmt.Print("| ")
		for i := 0; i < columnCount; i++ {
			for k := 0; k < (width[i] - len(dataout[i][j])); k++ {
				fmt.Print(" ")
			}
			fmt.Print(dataout[i][j])
			fmt.Print(" | ")
		}
		fmt.Println()
	}
}

//Create Attributes for the collection of the AttrInfo and the Data
//Collection of the AttrInfo and the Data
//Returns the AttrInfos and the Data of a Relation
func (rl *Relation) GetRawData() ([]interface{}, []AttrInfo) {
	//Create Attributes for the collection of the AttrInfo and the Data
	var sig = make([]AttrInfo, len(rl.Columns))
	var data = make([]interface{}, len(rl.Columns))
	//Collection of the AttrInfo and the Data
	for i := 0; i < len(rl.Columns); i++ {
		sig[i] = rl.Columns[i].Signature
		data[i] = rl.Columns[i].Data
	}
	return data, sig
}

//Not implemented yet
func (rl *Relation) HashJoin(col1 []AttrInfo, rightRelation string, col2 []AttrInfo, joinType JoinType, compType Comparison) Relationer {
	var ret Relation

	return &ret
}


func (rl *Relation) Aggregate(aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	
	if aggregate.Type == STRING && aggrFunc != COUNT {
		fmt.Println("Only COUNT for string supported")
		return rl
	}
	
	var aggrRelation Relation
	var aggrColumn Column
	var emptyColumn Column
	currentRow := make([]interface{}, 0)
	JoinColumns := make([]Column, 0)
	groupColumns := make([]Column, 0)
	groupSig := make([]AttrInfo, 0)
	//divide rl in groupColumns and aggrColumn

	for i := 0; i < len(rl.Columns); i++ {

		if rl.Columns[i].Signature != aggregate {

			groupColumns = append(groupColumns, rl.Columns[i])
			groupSig = append(groupSig, rl.Columns[i].Signature)
		} else {
			aggrColumn = rl.Columns[i]

		}
	}

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
		}	

	} //...and attrColumn if COUNT is used
	aggrPos := len(JoinColumns)
	if aggrFunc == COUNT {
		
		JoinColumns = append(JoinColumns, emptyColumn)
		JoinColumns[aggrPos].Signature = aggregate
		switch aggregate.Type {
		case INT:
			JoinColumns[aggrPos].Data = make([]int, 0)
		case FLOAT:
			JoinColumns[aggrPos].Data = make([]float64, 0)
		case STRING:
			JoinColumns[aggrPos].Data = make([]string, 0)
		}	
		aggrPos = aggrPos + 1
	}	
	
	JoinColumns = append(JoinColumns, emptyColumn)	
	JoinColumns[aggrPos].Signature.Name = "Aggregate"
	JoinColumns[aggrPos].Signature.Enc = NOCOMP

	if aggrFunc == COUNT || aggregate.Type == INT {
		JoinColumns[aggrPos].Signature.Type = INT
		JoinColumns[aggrPos].Data = make([]int, 0)	
	} else {JoinColumns[aggrPos].Signature.Type = FLOAT
		JoinColumns[aggrPos].Data = make([]float64, 0)	}
	
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
			}
		}
		if aggrFunc == COUNT {
			switch aggregate.Type {
				
			case INT:
				currentRow = append(currentRow, aggrColumn.Data.([]int)[i])
			case FLOAT:
				currentRow = append(currentRow, aggrColumn.Data.([]float64)[i])
			case STRING:
				currentRow = append(currentRow, aggrColumn.Data.([]string)[i])
			}
		}
		
		JoinColumnsLength := 0
		if len(JoinColumns) > 0 {
			switch JoinColumns[0].Signature.Type {
			case INT:
				JoinColumnsLength = len(JoinColumns[0].Data.([]int))
			case FLOAT:
				JoinColumnsLength = len(JoinColumns[0].Data.([]float64))
			case STRING:
				JoinColumnsLength = len(JoinColumns[0].Data.([]string))
			}
		}
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
					}
				}
			}
			//currentRow already in JoinColumns
			if PosAggrColumns == -1 {
				PosAggrColumns = posInJoin
				break
			}
		}
		switch aggregate.Type {
		case INT:
			
			//currentRow not in JoinColumns
			if PosAggrColumns == -2||PosAggrColumns == -1 {

				switch aggrFunc {
				case COUNT:
					currentRow = append(currentRow, 1)
				default:
					currentRow = append(currentRow, aggrColumn.Data.([]int)[i])
				}
				
				for j := 0; j < len(currentRow); j++ {
				
					switch JoinColumns[j].Signature.Type {
					case INT:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]int), currentRow[j].(int))
					case FLOAT:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]float64), currentRow[j].(float64))
					case STRING:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]string), currentRow[j].(string))
					}
				}

			//already in JoinColumns, update aggr value
			} else {
				
				switch aggrFunc {
				case COUNT:					
					JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] + 1
				case SUM:
					JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] + aggrColumn.Data.([]int)[i]
				case MAX:
					if JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] < aggrColumn.Data.([]int)[i] {							
						JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]  = aggrColumn.Data.([]int)[i]
					}
				case MIN:
					if JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]  > aggrColumn.Data.([]int)[i] {
						JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]  = aggrColumn.Data.([]int)[i]
					}
				}
			}
		case FLOAT:
				
			//currentRow not in JoinColumns
			if PosAggrColumns == -2||PosAggrColumns == -1 {

				switch aggrFunc {
				case COUNT:
					currentRow = append(currentRow, 1)
				default:
					currentRow = append(currentRow, aggrColumn.Data.([]float64)[i])
				}
				
				for j := 0; j < len(currentRow); j++ {
		
					switch JoinColumns[j].Signature.Type {
					case INT:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]int), currentRow[j].(int))
					case FLOAT:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]float64), currentRow[j].(float64))
					case STRING:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]string), currentRow[j].(string))
					}
				}

			//already in JoinColumns, update aggr value
			} else {
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
					}
				}
			}
		//only COUNT
		case STRING:
			if PosAggrColumns == -2||PosAggrColumns == -1 {					
				currentRow = append(currentRow, 1)				
				
				for j := 0; j < len(currentRow); j++ {
					switch JoinColumns[j].Signature.Type {
					case INT:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]int), currentRow[j].(int))
					case FLOAT:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]float64), currentRow[j].(float64))
					case STRING:
						JoinColumns[j].Data = append(JoinColumns[j].Data.([]string), currentRow[j].(string))
					}
				}
				
			//already in JoinColumns, add one to count
			} else {
				value := JoinColumns[aggrPos].Data.([]int)[PosAggrColumns]
				JoinColumns[aggrPos].Data.([]int)[PosAggrColumns] = value + 1
				}
			
		}		
	}
	aggrRelation.Name = "Aggregate"
	aggrRelation.Columns = JoinColumns

	return &aggrRelation

}