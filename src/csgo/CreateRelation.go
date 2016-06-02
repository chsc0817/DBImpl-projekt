package csgo

//Create a new Relation
//Create the number of Columns in the Relation
//Register the AttrInfo in the Columns
func (cs *ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	var rel Relation

	rel.Name = tabName
	//Create the number of Columns
	rel.Columns = make([]Column, len(sig))
	//Register the AttrInfo in the Columns
	for i := 0; i < len(sig); i++ {
		rel.Columns[i].Signature = sig[i]
	}
	return &rel
}