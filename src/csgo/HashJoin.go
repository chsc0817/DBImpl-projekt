package csgo

//HashJoin
//Herausuchen der Spalten
//Vergleich auf Gleichheit und ausführen des INNER Join
//Ohne Vergleich auf JoinType Und Comparison, weil jeweils nur einer verlangt war.
func (rl *Relation) HashJoin( col1 []AttrInfo, rightRelation Relationer, col2 []AttrInfo, joinType JoinType, compType Comparison ) Relationer {
	var rRelation = rightRelation.(*Relation)
	var sig = make( []AttrInfo, len( rl.Columns ) + len( rRelation.Columns ) )
	var ret Relation
	var columns1 int
	var columns2 int
	
	ret.Name = rl.Name + rRelation.Name
	for i := 0; i < len( rl.Columns ); i++ {
		sig[i] = rl.Columns[i].Signature;
		if( rl.Columns[i].Signature == col1[0] ) {
			columns1 = i;
			break;
		}
	}
	for i := 0; i < len( rRelation.Columns ); i++ {
		sig[len(rl.Columns)+i] = rRelation.Columns[i].Signature;
		if( rRelation.Columns[i].Signature == col2[0] ) {
			columns2 = i;
			break;
		}
	}
	for i := 0; i < ( len( rl.Columns ) + len( rRelation.Columns ) ); i++ {
		var column Column
		if i < len( rl.Columns ) {
			column.Signature = rl.Columns[i].Signature
		} else {
			column.Signature = rRelation.Columns[i-len( rl.Columns )].Signature
		}
		switch column.Signature.Type {
				case INT:
					column.Data = make([]int, 0)
				case FLOAT:
					column.Data = make([]float64, 0)
				case STRING:
					column.Data = make([]string, 0)
		}
		ret.Columns = append( ret.Columns, column )
	}
	for i := 0; i < interfacelen( rl.Columns[0].Data ); i++ {
		for j := 0; j < interfacelen( rRelation.Columns[0].Data ); j++ {
			//EQUAL + INNER
			switch rl.Columns[columns1].Signature.Type {
				case INT:
					if rl.Columns[columns1].Data.([]int)[i] == rRelation.Columns[columns2].Data.([]int)[j] {
						for k := 0; k < len( rl.Columns ); k++ {
							switch rl.Columns[k].Data.(type) {
								case []int:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]int), rl.Columns[k].Data.([]int)[i] )
								case []float64:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]float64), rl.Columns[k].Data.([]float64)[i] )
								case []string:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]string), rl.Columns[k].Data.([]string)[i] )
							}
						}
						for k := 0; k < len(rRelation.Columns); k++ {
							switch rRelation.Columns[k].Data.(type) {
								case []int:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]int), rRelation.Columns[k].Data.([]int)[j] )
								case []float64:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]float64), rRelation.Columns[k].Data.([]float64)[j] )
								case []string:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]string), rRelation.Columns[k].Data.([]string)[j] )
							}
						}
				}
				case FLOAT:
					if rl.Columns[columns1].Data.([]float64)[i] == rRelation.Columns[columns2].Data.([]float64)[j] {
						for k := 0; k < len( rl.Columns ); k++ {
							switch rl.Columns[k].Data.(type) {
								case []int:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]int), rl.Columns[k].Data.([]int)[i] )
								case []float64:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]float64), rl.Columns[k].Data.([]float64)[i] )
								case []string:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]string), rl.Columns[k].Data.([]string)[i] )
							}
						}
						for k := 0; k < len(rRelation.Columns); k++ {
							switch rRelation.Columns[k].Data.(type) {
								case []int:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]int), rRelation.Columns[k].Data.([]int)[j] )
								case []float64:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]float64), rRelation.Columns[k].Data.([]float64)[j] )
								case []string:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]string), rRelation.Columns[k].Data.([]string)[j] )
							}
						}
				}
				case STRING:
					if rl.Columns[columns1].Data.([]string)[i] == rRelation.Columns[columns2].Data.([]string)[j] {
						for k := 0; k < len( rl.Columns ); k++ {
							switch rl.Columns[k].Data.(type) {
								case []int:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]int), rl.Columns[k].Data.([]int)[i] )
								case []float64:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]float64), rl.Columns[k].Data.([]float64)[i] )
								case []string:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]string), rl.Columns[k].Data.([]string)[i] )
							}
						}
						for k := 0; k < len(rRelation.Columns); k++ {
							switch rRelation.Columns[k].Data.(type) {
								case []int:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]int), rRelation.Columns[k].Data.([]int)[j] )
								case []float64:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]float64), rRelation.Columns[k].Data.([]float64)[j] )
								case []string:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]string), rRelation.Columns[k].Data.([]string)[j] )
							}
						}
				}
			}
		}
	}
	
	return &ret
}