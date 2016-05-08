// Package csgo contains an interface for implementing an In-Memory Column
// Store in pure Go for teaching purposes.
package csgo

import (
	"os"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"path"
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

// Create a new Relation
func (cs ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relation {
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
func (cs ColumnStore) GetRelation(relName string) Relation {
	return cs.Relations[relName]
}

//	loads a csv file and returns it as a relation
//  the relation has the same name as the file
//	the first row is the tabName
//  the following rows are the Data
//  the second row defines the DataType	  	    
func (rl Relation) Load(csvFile string, separator rune) Relation {	
	var create_column Column
	file,_ := os.Open(csvFile)
	reader := csv.NewReader(file)
	reader.Comma = separator				
	record,err  := reader.Read()
	colName := record
	
	rl.Name = path.Base(csvFile)
	record,err = reader.Read()	
	
	for i:= 0; i < len(record); i++ {
		create_column.Signature.Name = colName[i]
		create_column.Signature.Type = GetType( record[i] )
		create_column.Signature.Enc = NOCOMP
		rl.Columns = append( rl.Columns, create_column )
		rl.Columns[i].Data = make( []interface{}, len( record ) )
	}
	fmt.Println( record )
	for j := 0; ; j++ {
		record,err = reader.Read()
		if err == io.EOF {
			break
		}
		
		if err != nil {
			fmt.Print( err )
			break
		}
		fmt.Println( record )
		for i:=0; i < len(record); i++ {
			fmt.Println( i )
			switch rl.Columns[i].Signature.Type {
				case INT: 
					rl.Columns[i].Data[j],_ = strconv.Atoi(record[i])								
					
				case FLOAT:
					rl.Columns[i].Data[j],_ = strconv.ParseFloat(record[i] ,64)
					
				case STRING:
					rl.Columns[i].Data[j].append( rl.Columns[i].Data[j], record[i] )
			}
		}				
	}
	fmt.Println( rl.Columns ) // ZU ENTFERNEN
	return rl
}

//Returns a Relation where the Columns are filtered by their AttrInfo
func (rl Relation) Scan(colList []AttrInfo) Relation {
	var ret Relation
	ret.Name = rl.Name
	//Test all Column if their AttrInfo is one of the wanted AttrInfo/Colums
	for i := 0; i < len(colList); i++ {
		for j := 0; j < len(rl.Columns); j++ {
			if rl.Columns[j].Signature == colList[i] {
				ret.Columns = append( ret.Columns, rl.Columns[j])
			}
		}
	}
	return ret
}


func (rl Relation) Select(col AttrInfo, comp Comparison, compVal interface{}) Relation {
	return rl
}


func (rl Relation) Print() {
	fmt.Println( rl.Name )
	fmt.Println()
	fmt.Println( len( rl.Columns ) )
	fmt.Println()
	for i := 0; i < len( rl.Columns ); i++ {
		fmt.Print( rl.Columns[i].Signature.Name + " | " )
	}
	fmt.Println()
	fmt.Println( "------------------------------------------------------------------------------------------------------------" )
	for i := 0; i < len( rl.Columns ); i++ {

	}
	//println( data )
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

func GetType(tabName string) DataTypes {
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