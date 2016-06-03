package csgo

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