package csgo

//Create Attributes for the collection of the AttrInfo and the Data
//Collection of the AttrInfo and the Data
//Returns the AttrInfos and the Data of a Relation
func (rl *Relation) GetRawData() ([]interface{}, []AttrInfo) {
	//Create Attributes for the collection of the AttrInfo and the Data
	var sig = make([]AttrInfo, len(rl.Columns))
	var data = make([]interface{}, len(rl.Columns))
	//Collection of the AttrInfo and the Data
	for i := 0; i < len(rl.Columns); i++ {
		sig[i] = rl.Columns[i].Signature
		data[i] = rl.Columns[i].Data
	}
	return data, sig
}