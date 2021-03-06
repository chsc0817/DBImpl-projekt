package csgo



//Returns the Number of the searched Column
func whichColumn( r *Relation, col []AttrInfo, c chan int) {
	colNum := -1
	
	for i := 0; i < len( r.Columns ); i++ {
		if( r.Columns[i].Signature == col[0] ) {
			colNum = i;
			break;
		}
	}
	c <- colNum
}

//Creates a new Relation
func CreateRelation( rl *Relation, rRelation *Relation, c chan Relation ) {
	var ret Relation
	ret.Name = rl.Name + rRelation.Name
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
	c <- ret
}

//String for Columns with Datatyp Int
func joinInt( h map[interface{}][]int, start int, stop int, columns2 int, rl *Relation, rRelation *Relation, c chan Relation ) {
	c2 := make(chan Relation)
	go CreateRelation( rl, rRelation, c2 )
	ret := <-c2
	for i := start; i < stop; i++ {
		for _, a := range h[rRelation.Columns[columns2].Data.([]int)[i]] {
			for k := 0; k < len( rl.Columns ); k++ {
							switch rl.Columns[k].Data.(type) {
								case []int:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]int), rl.Columns[k].Data.([]int)[a] )
								case []float64:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]float64), rl.Columns[k].Data.([]float64)[a] )
								case []string:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]string), rl.Columns[k].Data.([]string)[a] )
							}
						}
			for k := 0; k < len(rRelation.Columns); k++ {
							switch rRelation.Columns[k].Data.(type) {
								case []int:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]int), rRelation.Columns[k].Data.([]int)[i] )
								case []float64:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]float64), rRelation.Columns[k].Data.([]float64)[i] )
								case []string:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]string), rRelation.Columns[k].Data.([]string)[i] )
							}
						}
		}
	}
	c <- ret
}

//String for Columns with Datatyp FLOAT
func joinFloat( h map[interface{}][]int, start int, stop int, columns2 int, rl *Relation, rRelation *Relation, c chan Relation ) {
	c2 := make(chan Relation)
	go CreateRelation( rl, rRelation, c2 )
	ret := <-c2
	for i := start; i < stop; i++ {
		for _, a := range h[rRelation.Columns[columns2].Data.([]float64)[i]] {
		for k := 0; k < len( rl.Columns ); k++ {
							switch rl.Columns[k].Data.(type) {
								case []int:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]int), rl.Columns[k].Data.([]int)[a] )
								case []float64:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]float64), rl.Columns[k].Data.([]float64)[a] )
								case []string:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]string), rl.Columns[k].Data.([]string)[a] )
							}
						}
			for k := 0; k < len(rRelation.Columns); k++ {
							switch rRelation.Columns[k].Data.(type) {
								case []int:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]int), rRelation.Columns[k].Data.([]int)[i] )
								case []float64:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]float64), rRelation.Columns[k].Data.([]float64)[i] )
								case []string:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]string), rRelation.Columns[k].Data.([]string)[i] )
							}
						}
		}
	}
	c <- ret
}	

//String for Columns with Datatyp String
func joinString( h map[interface{}][]int, start int, stop int, columns2 int, rl *Relation, rRelation *Relation, c chan Relation ) {
	c2 := make(chan Relation)
	go CreateRelation( rl, rRelation, c2 )
	ret := <-c2
	for i := start; i < stop; i++ {
		for _, a := range h[rRelation.Columns[columns2].Data.([]string)[i]] {
			for k := 0; k < len( rl.Columns ); k++ {
							switch rl.Columns[k].Data.(type) {
								case []int:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]int), rl.Columns[k].Data.([]int)[a] )
								case []float64:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]float64), rl.Columns[k].Data.([]float64)[a] )
								case []string:
									ret.Columns[k].Data = append( ret.Columns[k].Data.([]string), rl.Columns[k].Data.([]string)[a] )
							}
						}
			for k := 0; k < len(rRelation.Columns); k++ {
							switch rRelation.Columns[k].Data.(type) {
								case []int:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]int), rRelation.Columns[k].Data.([]int)[i] )
								case []float64:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]float64), rRelation.Columns[k].Data.([]float64)[i] )
								case []string:
									ret.Columns[len( rl.Columns )+k].Data = append( ret.Columns[len(rl.Columns)+k].Data.([]string), rRelation.Columns[k].Data.([]string)[i] )
							}
						}
		}
	}
	c <- ret
}


//HashJoin
//Herausuchen der Spalten
//Vergleich auf Gleichheit und ausführen des INNER Join
//Ohne Vergleich auf JoinType Und Comparison, weil jeweils nur einer verlangt war.
func (rl *Relation) HashJoin( col1 []AttrInfo, rightRelation Relationer, col2 []AttrInfo, joinType JoinType, compType Comparison ) Relationer {
	var rRelation = rightRelation.(*Relation)
	
	c0 := make(chan int)
	c1 := make(chan int)
	go whichColumn( rl, col1,  c0)
	go whichColumn( rRelation, col2,  c1 )
	columns1 := <-c0
	columns2 := <-c1
    // hash phase
	c3 := make(chan Relation)
	switch col1[0].Type {
		case INT:
			h := map[interface{}][]int{}
			for i := 0; i < interfacelen(rl.Columns[0].Data); i++ {
				h[rl.Columns[columns1].Data.([]int)[i]] = append(h[rl.Columns[columns1].Data.([]int)[i]], i)
			}
			// join phase
			go joinInt( h, 0, ( interfacelen( rRelation.Columns[0].Data ) / 4 ), columns2, rl, rRelation, c3 )
			go joinInt( h, (interfacelen( rRelation.Columns[0].Data ) / 4), ( interfacelen( rRelation.Columns[0].Data ) / 2 ), columns2, rl, rRelation, c3 )
			go joinInt( h, ( interfacelen( rRelation.Columns[0].Data ) / 2 ), interfacelen( rRelation.Columns[0].Data ) * 3 / 4, columns2, rl, rRelation, c3 )
			go joinInt( h, (interfacelen( rRelation.Columns[0].Data ) *3 / 4), interfacelen( rRelation.Columns[0].Data ), columns2, rl, rRelation, c3 )
		case FLOAT:
			h := map[interface{}][]int{}
			for i := 0; i < interfacelen(rl.Columns[0].Data); i++ {
				h[rl.Columns[columns1].Data.([]float64)[i]] = append(h[rl.Columns[columns1].Data.([]float64)[i]], i)
			}
			// join phase
			go joinFloat( h, 0, ( interfacelen( rRelation.Columns[0].Data ) / 4 ), columns2, rl, rRelation, c3 )
			go joinFloat( h, (interfacelen( rRelation.Columns[0].Data ) / 4), ( interfacelen( rRelation.Columns[0].Data ) / 2 ), columns2, rl, rRelation, c3 )
			go joinFloat( h, ( interfacelen( rRelation.Columns[0].Data ) / 2 ), interfacelen( rRelation.Columns[0].Data ) * 3 / 4, columns2, rl, rRelation, c3 )
			go joinFloat( h, (interfacelen( rRelation.Columns[0].Data ) *3 / 4), interfacelen( rRelation.Columns[0].Data ), columns2, rl, rRelation, c3 )
		case STRING:
			h := map[interface{}][]int{}
			for i := 0; i < interfacelen(rl.Columns[0].Data); i++ {
				h[rl.Columns[columns1].Data.([]string)[i]] = append(h[rl.Columns[columns1].Data.([]string)[i]], i)
			}
			// join phase
			go joinString( h, 0, ( interfacelen( rRelation.Columns[0].Data ) / 4 ), columns2, rl, rRelation, c3 )
			go joinString( h, (interfacelen( rRelation.Columns[0].Data ) / 4), ( interfacelen( rRelation.Columns[0].Data ) / 2 ), columns2, rl, rRelation, c3 )
			go joinString( h, ( interfacelen( rRelation.Columns[0].Data ) / 2 ), interfacelen( rRelation.Columns[0].Data ) * 3 / 4, columns2, rl, rRelation, c3 )
			go joinString( h, (interfacelen( rRelation.Columns[0].Data ) *3 / 4), interfacelen( rRelation.Columns[0].Data ), columns2, rl, rRelation, c3 )
	}
	ret,ret2,ret3,ret4 := <-c3,<-c3,<-c3,<-c3
	//merge/join the to relation
	for i := 0; i < (len( rl.Columns ) + len( rRelation.Columns )); i++ {
		switch ret.Columns[i].Data.(type) {
			case []int :	
				for j := 0; j < interfacelen(ret2.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]int), ret2.Columns[i].Data.([]int)[j] )
				}
				for j := 0; j < interfacelen(ret3.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]int), ret3.Columns[i].Data.([]int)[j] )
				}
				for j := 0; j < interfacelen(ret4.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]int), ret4.Columns[i].Data.([]int)[j] )
				}
			case []float64 :
				for j := 0; j < interfacelen(ret2.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]float64), ret2.Columns[i].Data.([]float64)[j] )
				}
				for j := 0; j < interfacelen(ret3.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]float64), ret3.Columns[i].Data.([]float64)[j] )
				}
				for j := 0; j < interfacelen(ret4.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]float64), ret4.Columns[i].Data.([]float64)[j] )
				}
				case []string :
				for j := 0; j < interfacelen(ret2.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]string), ret2.Columns[i].Data.([]string)[j] )
				}
				for j := 0; j < interfacelen(ret3.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]string), ret3.Columns[i].Data.([]string)[j] )
				}
				for j := 0; j < interfacelen(ret4.Columns[i].Data); j++ {
					ret.Columns[i].Data = append( ret.Columns[i].Data.([]string), ret4.Columns[i].Data.([]string)[j] )
				}
		}
	}
	return &ret
}