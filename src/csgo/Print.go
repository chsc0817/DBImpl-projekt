package csgo

import "fmt"

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