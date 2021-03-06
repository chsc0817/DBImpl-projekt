// Package csgo contains an interface for implementing an In-Memory Column
// Store in pure Go for teaching purposes.
package csgo

import (
	"os"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)



// TODO: Session 1 - Implement the Relationer and ColumnStorer interface by
// using e.g. the Relation and ColumnStore struct (i.e. all method signatures/
// heads in a separte file). Implement Load, Scan, Select, Print, GetRawData,
// CreateRelation and GetRelation.
// TODO: Session 2 - Implement HashJoin and Aggregate
// TODO: Session 3 - Parallisation and Acceleration

// Comparison is an enum type for all possible comparison operations used e.g.
// for Select predicates.
type Comparison string

const (
	// EQ is the "equality" comparison operation.
	EQ Comparison = "=="
	// NEQ is the "negative equality" comparison operation.
	NEQ Comparison = "!="
	// LT is the "lesser than" comparison operation.
	LT Comparison = "<"
	// LEQ is the "lesser equal than" comparison operation.
	LEQ Comparison = "<="
	// GT is the "greater than" comparison operation.
	GT Comparison = ">"
	// GEQ is the "greater equal than" comparison operation.
	GEQ Comparison = ">="
)

// Compression is an enumeration type for all supported column encoding methods.
type Compression int

const (
	// NOCOMP means that no encoding method is used.
	NOCOMP Compression = iota
	// RLE is the run-length encoding method.
	RLE
	// DICT is the dictionary encoding method.
	DICT
	// FOR is the frame of reference encoding method.
	FOR
)

// JoinType defines all supported types of join.
type JoinType int

const (
	// EQUI is an inner join allowing only equality comparisons between columns.
	EQUI JoinType = iota
	// SEMI behaves like a natural join, but only projects the reduction of the
	// left relation.
	SEMI
	// LEFTOUTER returns all records of the left relation and possibly matching
	// records of the right relation.
	LEFTOUTER
	// RIGHTOUTER returns all records of the right relation and possibly matching
	// records of the left relation.
	RIGHTOUTER
)

// AggrFunc is an enumeration type for all predefined functions of aggregation.
type AggrFunc int

const (
	// COUNT retuns the number of all elements of a collection.
	COUNT AggrFunc = iota
	// SUM returns the sum of values for all elements of a collection.
	SUM
	// MIN returns the lowest value for all elements of a collection.
	MIN
	// MAX returns the highest value for all elements of a collection.
	MAX
)

// DataTypes is the enumeration of all supported column data types
type DataTypes int

const (
	// INT represents the integer data type
	INT DataTypes = iota
	// FLOAT represents the decimal data type
	FLOAT
	// STRING represents the character string data type
	STRING
)

// AttrInfo contains meta information about a column (name and type).
type AttrInfo struct {
	// Name is the name of the column.
	Name string
	// Type is the type of the column (int, float or string).
	Type DataTypes
	// Enc defines the encoding of this column.
	Enc Compression
}

// Column is a single column containing the signature and the payload.
type Column struct {
	// Signature gives meta information about the column.
	Signature AttrInfo
	// Data contains the raw or compressed data (e.g. in the form of a slice).
	Data []interface{}
}

// Relation is an example structure on which one could define the Relationer
// methods.
type Relation struct {
	// Name is the name of the relation as string representation.
	Name string
	// Columns is the collection of all columns of this relation.
	Columns []Column
}

// Relationer is an interface for a table/relation within a ColumnStore.
type Relationer interface {
	// Load should load and insert the data of a CSV file into the column store.
	// csvFile is the path to the CSV File.
	// separator is separator character used in the file.
	Load(csvFile string, separator rune) Relation

	// Scan should simply return the specified columns of the relation.
	Scan(colList []AttrInfo) Relation

	// Select should return a filtered collection of records defined by predicate
	// arguments (col, comp, compVal) of one relation.
	// col represents the column used for comparison.
	// comp defines the type of comparison.
	// compVal is the value used for the comparison.
	Select(col AttrInfo, comp Comparison, compVal interface{}) Relation

	// Print should output the relation to the standard output in record
	// representation.
	Print()

	// GetRawData should return all columns as a slice of slices (columns) with
	// the underlying type (int, float, string) in decompressed form and the
	// corresponding meta information.
	GetRawData() ([][]interface{}, []AttrInfo)

	// HashJoin should implement the hash join operator between two relations.
	// joinType specifies the kind of hash join (inner, outer, semi ...)
	// The join may be executed on one or more columns of each relation.
	HashJoin(col1 []AttrInfo, input2 []Column, col2 []AttrInfo, joinType JoinType) Relation

	// Aggregate should implement the grouping and aggregation of columns.
	// groupBy specifies on which columns it should be grouped.
	// aggregate defines the column on which the aggrFunc should be applied.
	Aggregate(groupBy []AttrInfo, aggregate AttrInfo, aggrFunc AggrFunc) Relation
}

// ColumnStore is an example structure on which one could define the
// ColumnStorer methods.
type ColumnStore struct {
	// Relations is the mapping of relation names to their object reference.
	Relations map[string]Relation
}

// ColumnStorer is an interface for an In-Memory Column Store (the database).
type ColumnStorer interface {
	// CreateRelation creates a new relation within the column store and returns
	// an object reference.
	CreateRelation(tabName string, sig []AttrInfo) Relation
	// GetRelation returns the object reference of a relation associated with the
	// passed relation name.
	GetRelation(relName string) Relation
}

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

func NewColumnStore() ColumnStore {
	var cs ColumnStore
	cs.Relations = make(map[string]Relation)
	return cs
}

// Create a new Relation
func (cs ColumnStore) CreateRelation( tabName string, sig []AttrInfo ) Relation {
	//Create the number of Columns
	var cl = make( []Column, len( sig ) )
	//Register the AttrInfo in the Columns
	for i := 0; i < len( sig ) ; i++ {
		cl[i].Signature = sig [i]
	}
	//Creating the Relation out of the Name an the Columns
	cs.Relations[tabName] = Relation{ tabName, cl }
	return cs.Relations[tabName]
}

//Returns the Relation
func (cs ColumnStore) GetRelation( relName string ) Relation {
	return cs.Relations[relName]
}

//	loads a csv file and returns it as a relation
//  the relation has the same name as the file
//	the first row is the tabName
//  the following rows are the Data
//  the second row defines the DataType	  	    
func (rl Relation) Load( csvFile string, separator rune ) Relation {	
	//var create_column Column
	file,_ := os.Open( csvFile )
	reader := csv.NewReader( file )
	reader.Comma = separator				
	record,err  := reader.Read()
	
	for j := 0; ; j++ {
		if err == io.EOF {
			break
		}
		
		if err != nil {
			fmt.Print( err )
			break
		}
		
		for i:=0; i < len( record ); i++ {
			//Reading in the data by their type
			switch rl.Columns[i].Signature.Type {
				case INT: 
					datas,_ := strconv.Atoi( record[i] )								
					rl.Columns[i].Data = append( rl.Columns[i].Data, datas ) 
			
				case FLOAT:
					datas,_ := strconv.ParseFloat( record[i], 64 )
					rl.Columns[i].Data = append( rl.Columns[i].Data, datas ) 
				
				case STRING:
					rl.Columns[i].Data = append( rl.Columns[i].Data, record[i] )
			}
		}
		record,err = reader.Read()
	}
	return rl
}


//Returns a Relation where the Columns are filtered by their AttrInfo
func (rl Relation) Scan( colList []AttrInfo ) Relation {
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
	return ret
}

//Filter the Relation for records
func (rl Relation) Select( col AttrInfo, comp Comparison, compVal interface{} ) Relation {
	var colu int
	var ret Relation
	var create_column Column
	
	//Create the new Relation + Columns and search the Column with which we shall compare
	ret.Name = rl.Name
	for i := 0; i < len( rl.Columns ); i++ {
		create_column.Signature = rl.Columns[i].Signature
		ret.Columns = append( ret.Columns, create_column )
		if rl.Columns[i].Signature == col {
			colu = i
		}
	}
	//Compare the data and the searched Value and put the right ones in the new Relation
	for i := 0; i < len( rl.Columns[0].Data ); i++ {
		switch comp {
			case EQ :
				if rl.Columns[colu].Data[i] == compVal {
					for j := 0; j < len( rl.Columns ); j++ {
						ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
					}
				}
			case NEQ :
				if rl.Columns[colu].Data[i] != compVal {
					for j := 0; j < len( rl.Columns ); j++ {
						ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
					}
				}
			case LT :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data[i].(int) < compVal.(int) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
					case FLOAT :
						if rl.Columns[colu].Data[i].(float64) < compVal.(float64) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
				}
			case LEQ :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data[i].(int) <= compVal.(int) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
					case FLOAT :
						if rl.Columns[colu].Data[i].(float64) <= compVal.(float64) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
				}
			case GT :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data[i].(int) > compVal.(int) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
					case FLOAT :
						if rl.Columns[colu].Data[i].(float64) > compVal.(float64) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
				}
			case GEQ :
				switch rl.Columns[colu].Signature.Type {
					case INT :
						if rl.Columns[colu].Data[i].(int) >= compVal.(int) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
					case FLOAT :
						if rl.Columns[colu].Data[i].(float64) >= compVal.(float64) {
							for j := 0; j < len( rl.Columns ); j++ {
								ret.Columns[j].Data = append( ret.Columns[j].Data, rl.Columns[j].Data[i] )
							}
						}
				}
		}
	}
	fmt.Println( ret.Columns )
	return ret
}

//Converts the Interface to a String
func interfaceToString( inputInterface interface{} ) string {
	switch inputInterface.(type) {
		case int,int32,int64 :
            return strconv.Itoa( inputInterface.(int) )
		case float64 :
            return strconv.FormatFloat( inputInterface.(float64), 'E', -1, 64)
		//string
		default :
            return inputInterface.(string)
	}
}

//Prints the relation
func (rl Relation) Print() {
	var dataout = make( [][]string, len( rl.Columns ) )
	var metaout string
	var width = make( []int, len( rl.Columns ) )
	var dataSetCount int
	var columnCount int

	//fmt.Println( rl.Name )
	//fmt.Println()
	dataSetCount = len( rl.Columns[0].Data )
	columnCount = len( rl.Columns )
	//Fetching  the data
	for i := 0; i < len( rl.Columns ); i++ {
		dataout[i] = append( dataout[i], rl.Columns[i].Signature.Name )
	}
	for j := 0; j < dataSetCount; j++ {
		for i := 0; i < columnCount; i++ {
			dataout[i] = append( dataout[i], interfaceToString( rl.Columns[i].Data[j] ) )
		}
	}
	//testing for the max width of the strings
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

//Returns the AttrInfos and the Data of a Relation
func (rl Relation) GetRawData() ([]interface{}, []AttrInfo) {
	//Create Attributes for the collection of the AttrInfo and the Data
	var sig = make( []AttrInfo, len( rl.Columns ) )
	var data = make( []interface{}, len( rl.Columns ) )
	//Collection of the AttrInfo and the Data
	for i := 0; i < len( rl.Columns ); i++ {
		sig[i] = rl.Columns[i].Signature
		data[i] = rl.Columns[i].Data
	}
	fmt.Println( sig )
	fmt.Println( data )
	return data, sig
}

func getType( tabName string ) DataTypes {
	_,err := strconv.Atoi(tabName)
	
	if err != nil {
		_, err := strconv.ParseFloat(tabName, 64)
		
		if err != nil {
			return STRING			
		}
		
		return FLOAT
	}
	
	return INT	
}