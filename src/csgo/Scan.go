package csgo

//Test all Column if their AttrInfo is one of the wanted AttrInfo/Colums
//Returns a Relation where the Columns are filtered by their AttrInfo
func (rl *Relation) Scan(colList []AttrInfo) Relationer {
	var ret Relation
	ret.Name = rl.Name
	//Test all Column if their AttrInfo is one of the wanted AttrInfo/Colums
	for i := 0; i < len(colList); i++ {
		for j := 0; j < len(rl.Columns); j++ {
			if rl.Columns[j].Signature == colList[i] {
				ret.Columns = append(ret.Columns, rl.Columns[j])
			}
		}
	}
	return &Relation{ret.Name, ret.Columns}
}