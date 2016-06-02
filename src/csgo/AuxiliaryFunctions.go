package csgo

import (
	"strconv"
)

//Creating a ColumnStore for test purposes
func NewColumnStore() ColumnStorer {
	var cs ColumnStore
	cs.Relations = make(map[string]Relationer)
	return &cs
}

//Returns the pointer on the Relation
func (cs *ColumnStore) GetRelation(relName string) Relationer {
	return cs.Relations[relName]
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