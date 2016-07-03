package csgo

import "fmt"

func CompareColumn( rl *Relation, col AttrInfo, comp Comparison, compVal interface{}, colu int, start int, stop int, c chan Relation ) {
	var ret Relation
	var create_column Column
	
	for i := 0; i < len(rl.Columns); i++ {
		create_column.Signature = rl.Columns[i].Signature
		ret.Columns = append(ret.Columns, create_column)
		if rl.Columns[i].Signature == col {
			colu = i
		}
	}
	for i := start; i < stop; i++ {
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
	fmt.Println(ret)
	c <- ret
}



//Filter the Relation for records
//Create the new Relation + Columns and search the Column with which we shall compare
//Compare the data and the searched Value and put the right ones in the new Relation
//Strings are only compared on equality and  inequality
func (rl *Relation) Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer {
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
		switch create_column.Signature.Type {
			case INT:
				create_column.Data = make( []int, 0)
			case FLOAT:
				create_column.Data = make( []float64, 0)
			case STRING:
				create_column.Data = make( []string, 0)
		}
		ret.Columns = append(ret.Columns, create_column)
		if rl.Columns[i].Signature == col {
			colu = i
		}
	}
	//Compare the data and the searched Value and put the right ones in the new Relation
	c3 := make(chan Relation)
	go CompareColumn( rl, col, comp, compVal, colu, 0, interfacelen( rl.Columns[0].Data ) / 2, c3 )
	go CompareColumn( rl, col, comp, compVal, colu, interfacelen( rl.Columns[0].Data ) / 2, interfacelen( rl.Columns[0].Data ), c3 )
	ret1,ret2 := <-c3,<-c3
	fmt.Println(ret1)
	fmt.Println(ret2)
	for i := 0; i < len( rl.Columns ); i++ {
		switch rl.Columns[i].Data.(type) {
			case []int :
				for j := 0; j < interfacelen(ret1.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]int), ret1.Columns[i].Data.([]int)[j] )
				}	
				for j := 0; j < interfacelen(ret2.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]int), ret2.Columns[i].Data.([]int)[j] )
				}
			case []float64 :
				for j := 0; j < interfacelen(ret1.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]float64), ret1.Columns[i].Data.([]float64)[j] )
				}
				for j := 0; j < interfacelen(ret2.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]float64), ret2.Columns[i].Data.([]float64)[j] )
				}
				case []string :
				for j := 0; j < interfacelen(ret1.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]string), ret1.Columns[i].Data.([]string)[j] )
				}
				for j := 0; j < interfacelen(ret2.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]string), ret2.Columns[i].Data.([]string)[j] )
				}
		}
	}
	return &ret
}