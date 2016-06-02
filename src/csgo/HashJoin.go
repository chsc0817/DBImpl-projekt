package csgo

//Not implemented yet
func (rl *Relation) HashJoin( col1 []AttrInfo, rightRelation string, col2 []AttrInfo, joinType JoinType, compType Comparison ) Relationer {
	rightRelation1 = cs.getRelation(rightRelation)
	var sig = make( []AttrInfo, len(rl.Columns) + len(rightRelation1.Columns) )
	var colums1 int
	var colums2 int
	
	for i := 0; i < len(rl.Columns); i++ {
		sig[i] = rl.Columns[i].AttrInfo;
		if( rl.Columns[i].AttrInfo == col1[0] ) {
			colums1 = i;
			break;
		}
	}
	for i := 0; i < len(rightRelation1.Columns); i++ {
		sig[len(rl.Columns)+i] = rightRelation1.Columns[i].AttrInfo;
		if( rightRelation1.Columns[i].AttrInfo == col2[0] ) {
			colums2 = i;
			break;
		}
	}
	var ret = Relationer.CreateRelation(rl.Name + rightRelation1.Name, sig);
	for i := 0; i < len(rl.Columns); i++ {
		for j := 0; j < len(rightRelation1.Columns); j++ {
			//EQUAL + INNER
			if rl.Columns[colums1].Data[i] == rightRelation1.Columns[colums2].Data[j] {
				for k := 0; k < len(rl.Columns); k++ {
					ret.Columns[k].Data = append( ret.Columns[k].Data, rl.Columns[k].Data )
				}
				for k := 0; k < len(rightRelation1.Columns); k++ {
					ret.Columns[len(rl.Columns)+k].Data = append( ret.Columns[len(rl.Columns)+k].Data, rightRelation1.Columns[k].Data )
				}
			} 
		}
	}
	
	return &ret
}