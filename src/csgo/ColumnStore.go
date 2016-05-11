package csgo

import (
	"os"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"path"
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
func (cs *ColumnStore) CreateRelation( tabName string, sig []AttrInfo ) Relationer {
	var rel Relation
	
	rel.Name = tabName
	//Create the number of Columns
	rel.Columns = make( []Column, len( sig ) )
	//Register the AttrInfo in the Columns
	for i := 0; i < len( sig ) ; i++ {
		rel.Columns[i].Signature = sig [i]
	}
	return &rel
}

//Returns the pointer on the Relation
func (cs *ColumnStore) GetRelation( relName string ) Relationer {
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
func (rl *Relation) Load( csvFile string, separator rune ) {	
	file,err := os.Open(csvFile)
	
	//Error if file could not be opened or found
	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}
		
	reader := csv.NewReader(file)
	reader.Comma = separator
	record,err  := reader.Read()
	
	//File hat not the same number of Columns than the Relation.
	if len(record) != len(rl.Columns) {
		fmt.Println("Number of defined columns is different than the number of columns in ", path.Base(csvFile))
		os.Exit(1)
	}
	//Create the data slices and insert the first row
	for i:= 0; i < len(record); i++ {		
		switch rl.Columns[i].Signature.Type {
			case INT: 
				rl.Columns[i].Data = make([]int, 0)
				datas,err := strconv.Atoi(record[i])	
				
				//Error if inputType is not int
				if err != nil {
					fmt.Println("error while loading", path.Base(csvFile))
					fmt.Print("row ", len(rl.Columns[i].Data.([]int))+1, ", column ", i+1, ": ")
					fmt.Print("\"", record[i], "\"")
					fmt.Println(" is not type int")
					os.Exit(1)
				}
				rl.Columns[i].Data = append( rl.Columns[i].Data.([]int), datas ) 
		
			case FLOAT:
				rl.Columns[i].Data = make([]float64, 0)
				datas,err := strconv.ParseFloat( record[i], 64 )
				
				//Error if inputType is not float
				if err != nil {
					fmt.Println("error while loading", path.Base(csvFile))
					fmt.Print("row ", len(rl.Columns[i].Data.([]int))+1, ", column ", i+1, ": ")
					fmt.Print("\"", record[i], "\"")
					fmt.Println(" is not type float")
					os.Exit(1)
				}
				rl.Columns[i].Data = append( rl.Columns[i].Data.([]float64), datas ) 
			
			case STRING:
				rl.Columns[i].Data = make([]string, 0)
				rl.Columns[i].Data = append( rl.Columns[i].Data.([]string), record[i] )
		}	
	}	

	for {
		record,err = reader.Read()
		
		//Breaks at the end of the file
		if err == io.EOF {
			break
		}
		
		//Error if file could not be read
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}
		
		for i:=0; i < len(record); i++ {
			//Reading in the data by their type
			switch rl.Columns[i].Signature.Type {
				case INT: 
					datas,err := strconv.Atoi(record[i])
					
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
					datas,err := strconv.ParseFloat(record[i], 64)
					
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
func (rl *Relation) Scan( colList []AttrInfo ) Relationer {
	var ret Relation
	ret.Name = rl.Name
	//Test all Column if their AttrInfo is one of the wanted AttrInfo/Colums
	for i := 0; i < len( colList ); i++ {
		for j := 0; j < len( rl.Columns ); j++ {
			if rl.Columns[j].Signature == colList[i] {
				ret.Columns = append( ret.Columns, rl.Columns[j] )
			}
		}
	}
	return &Relation{ ret.Name, ret.Columns }
}

//Determine the length of a slice in an interface
func interfacelen( inter interface{} ) int {
	switch inter.(type) {
		case []int :
			return len( inter.([]int) )
		case []float64 :
			return len( inter.([]float64) )
		case []string :
			return len( inter.([]string) )
	}
	return 0
}

//Test if the Columns are declared and declare them if they're not
//Copys from the inputColumns into the outputColumns
func copyColumns( outputColumns []Column, inputColumns []Column, record int ) []Column {
	//Test if the Columns are declared and declare them if they're not
	for j := 0; j < len( inputColumns ); j++ {
		if nil == outputColumns[j].Data { 
			switch inputColumns[j].Data.(type) {
				case []int :
					outputColumns[j].Data = make( []int, 0 ) 
				case []float64 :
					outputColumns[j].Data = make( []float64, 0 ) 
				case []string :
					outputColumns[j].Data = make( []string, 0 ) 
			}
		}
	}
	//Copys the inputColumns into the outputColumns
	for j := 0; j < len( inputColumns ); j++ {
		switch inputColumns[j].Data.(type) {
			case []int :
				outputColumns[j].Data = append( outputColumns[j].Data.([]int), inputColumns[j].Data.([]int)[record] )
			case []float64 :
				outputColumns[j].Data = append( outputColumns[j].Data.([]float64), inputColumns[j].Data.([]float64)[record] )
			case []string :
				outputColumns[j].Data = append( outputColumns[j].Data.([]string), inputColumns[j].Data.([]string)[record] )
		}
	}
	return outputColumns
}

//Filter the Relation for records
//Create the new Relation + Columns and search the Column with which we shall compare
//Compare the data and the searched Value and put the right ones in the new Relation
//Strings are only compared on equality and  inequality 
func (rl Relation) Select( col AttrInfo, comp Comparison, compVal interface{} ) Relationer {
	var colu int
	var ret Relation
	var create_column Column
	
	//Create the new Relation + Columns and search the Column with which we shall compare
	ret.Name = rl.Name
	//If the Relation has no Columns return
	if( 0 == len( rl.Columns ) ) {
		return &ret
	}
	for i := 0; i < len( rl.Columns ); i++ {
		create_column.Signature = rl.Columns[i].Signature
		ret.Columns = append( ret.Columns, create_column )
		if rl.Columns[i].Signature == col {
			colu = i
		}
	}
	//Compare the data and the searched Value and put the right ones in the new Relation
	for i := 0; i < interfacelen( rl.Columns[0].Data ); i++ {
		switch comp {
			case EQ :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data.([]int)[i] == compVal.(int) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case FLOAT :
						if rl.Columns[colu].Data.([]float64)[i] == compVal.(float64) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case STRING :
						if rl.Columns[colu].Data.([]string)[i] == compVal.(string) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
				}
			case NEQ :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data.([]int)[i] != compVal.(int) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case FLOAT :
						if rl.Columns[colu].Data.([]float64)[i] != compVal.(float64) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case STRING :
						if rl.Columns[colu].Data.([]string)[i] != compVal.(string) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
				}
			case LT :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data.([]int)[i] < compVal.(int) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case FLOAT :
						if rl.Columns[colu].Data.([]float64)[i] < compVal.(float64) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
				}
			case LEQ :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data.([]int)[i] <= compVal.(int) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case FLOAT :
						if rl.Columns[colu].Data.([]float64)[i] <= compVal.(float64) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
				}
			case GT :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data.([]int)[i] > compVal.(int) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case FLOAT :
						if rl.Columns[colu].Data.([]float64)[i] > compVal.(float64) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
				}
			case GEQ :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data.([]int)[i] >= compVal.(int) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
					case FLOAT :
						if rl.Columns[colu].Data.([]float64)[i] >= compVal.(float64) {
							ret.Columns = copyColumns( ret.Columns, rl.Columns, i )
						}
				}
		}
	}
	return &ret
}

//Converts the Interface to a String
func interfaceToString( inputInterface interface{}, j int ) string {
	switch inputInterface.(type) {
		case []int :
            return strconv.Itoa( inputInterface.([]int)[j] )
		case []float64 :
            return strconv.FormatFloat( inputInterface.([]float64)[j], 'E', -1, 64)
		//string
		default :
            return inputInterface.([]string)[j]
	}
}

//Prints the relation
//Fetching  the data
//Print the column names
//Testing for the max width of the strings
//Print the datas in the columns
func (rl *Relation) Print() {
	var dataout = make( [][]string, len( rl.Columns ) )
	var metaout string
	var width = make( []int, len( rl.Columns ) )
	var dataSetCount int
	var columnCount int

	//fmt.Println( rl.Name )
	//fmt.Println()
	columnCount = len( rl.Columns )
	//If the number of Columns 0 than return
	if 0 == columnCount {
		return
	}
	dataSetCount = interfacelen( rl.Columns[0].Data )
	//Fetching  the data
	for i := 0; i < len( rl.Columns ); i++ {
		dataout[i] = append( dataout[i], rl.Columns[i].Signature.Name )
	}
	for j := 0; j < dataSetCount; j++ {
		for i := 0; i < columnCount; i++ {
			dataout[i] = append( dataout[i], interfaceToString( rl.Columns[i].Data, j ) )
		}
	}
	//Testing for the max width of the strings
	for i := 0; i < columnCount; i++ {
		width[i] = 0
		for j := 0; j < dataSetCount; j++ {
			if len( dataout[i][j] ) > width[i] {
				width[i] = len( dataout[i][j] )
			}
		}
	}	
	//Print the column names
	metaout = "| "
	for i := 0; i < columnCount; i++ {
		for j := 0; j < ( width[i] - len( dataout[i][0] ) ); j++ {
			metaout = metaout + " "
		}
		metaout = metaout + dataout[i][0] + " | "
	}
	fmt.Println( metaout )
	for i := 0; i < len( metaout ) - 1; i++ {
		fmt.Print( "-" )
	}
	fmt.Println()
	//Print the datas in the columns
	for j := 1; j < dataSetCount + 1; j++ {
		fmt.Print( "| " )
		for i := 0; i < columnCount; i++ {
			for k := 0; k < ( width[i] - len( dataout[i][j] ) ); k++ {
				fmt.Print( " " )
			}
			fmt.Print( dataout[i][j] )
			fmt.Print( " | " )
		}
		fmt.Println()
	}
}

//Create Attributes for the collection of the AttrInfo and the Data
//Collection of the AttrInfo and the Data
//Returns the AttrInfos and the Data of a Relation
func (rl *Relation) GetRawData() ([]interface{}, []AttrInfo) {
	//Create Attributes for the collection of the AttrInfo and the Data
	var sig = make( []AttrInfo, len( rl.Columns ) )
	var data = make( []interface{}, len( rl.Columns ) )
	//Collection of the AttrInfo and the Data
	for i := 0; i < len( rl.Columns ); i++ {
		sig[i] = rl.Columns[i].Signature
		data[i] = rl.Columns[i].Data
	}
	return data, sig
}
