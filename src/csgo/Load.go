package csgo

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

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
		panic(err)
	}

	reader := csv.NewReader(file)
	reader.Comma = separator
	record, err := reader.Read()

	//File hat not the same number of Columns than the Relation.
	if len(record) != len(rl.Columns) {
		panic(fmt.Sprintf("Number of defined columns is different than the number of columns in "+path.Base(csvFile)))
	}
	//Create the data slices and insert the first row
	for i := 0; i < len(record); i++ {
		switch rl.Columns[i].Signature.Type {
		case INT:
			rl.Columns[i].Data = make([]int, 0)
			datas, err := strconv.Atoi(record[i])

			//Error if inputType is not int
			if err != nil {			
				panic(fmt.Sprintf("error while loading "+path.Base(csvFile)+"\n"+"row "+strconv.Itoa(len(rl.Columns[i].Data.([]int))+1)+", column "+strconv.Itoa(i+1)+": "+"\n"+fmt.Sprintf(record[i])+" is not type int"))
			}
			rl.Columns[i].Data = append(rl.Columns[i].Data.([]int), datas)

		case FLOAT:
			rl.Columns[i].Data = make([]float64, 0)
			datas, err := strconv.ParseFloat(record[i], 64)

			//Error if inputType is not float
			if err != nil {
				panic(fmt.Sprintf("error while loading "+path.Base(csvFile)+"\n"+"row "+strconv.Itoa(len(rl.Columns[i].Data.([]int))+1)+", column "+strconv.Itoa(i+1)+": "+"\n"+fmt.Sprintf(record[i])+" is not type float"))
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
			panic(err)
		}

		for i := 0; i < len(record); i++ {
			//Reading in the data by their type
			switch rl.Columns[i].Signature.Type {
			case INT:
				datas, err := strconv.Atoi(record[i])

				//Error if inputType is not int
				if err != nil {
					panic(fmt.Sprintf("error while loading "+path.Base(csvFile)+"\n"+"row "+strconv.Itoa(len(rl.Columns[i].Data.([]int))+1)+", column "+strconv.Itoa(i+1)+": "+"\n"+fmt.Sprintf(record[i])+" is not type int"))
				}
				rl.Columns[i].Data = append(rl.Columns[i].Data.([]int), datas)

			case FLOAT:
				datas, err := strconv.ParseFloat(record[i], 64)

				//Error if inputType is not float
				if err != nil {
					panic(fmt.Sprintf("error while loading "+path.Base(csvFile)+"\n"+"row "+strconv.Itoa(len(rl.Columns[i].Data.([]int))+1)+", column "+strconv.Itoa(i+1)+": "+"\n"+fmt.Sprintf(record[i])+" is not type float"))
				}
				rl.Columns[i].Data = append(rl.Columns[i].Data.([]float64), datas)

			case STRING:
				rl.Columns[i].Data = append(rl.Columns[i].Data.([]string), record[i])
			}
		}
	}
}