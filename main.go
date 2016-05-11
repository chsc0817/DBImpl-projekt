package main


//import "C:/Uni/Projekte/DBImpl-projekt/interfaces.go"
import "csgo"
//import "fmt"
 
func main() {
	/*var name string
	var sig []csgo.AttrInfo
	var cs csgo.ColumnStore
	
	cs.Relations = make(map[string]csgo.Relation)
	name = "NewTable"
	cs.CreateRelation( name, sig )
    cs.Relations[name] = cs.Relations[name].Load( "C:/Uni/Projekte/Ausgabe/nation.tbl", 0x7C )
	cs.Relations[name].Print()*/
	//for ours (backup)
	/*var cs csgo.ColumnStore
	cs = csgo.NewColumnStore()
	var sig []csgo.AttrInfo
	

	partkey := csgo.AttrInfo{Name: "ps_partkey", Type: csgo.INT, Enc: csgo.NOCOMP}
	suppkey := csgo.AttrInfo{Name: "ps_suppkey", Type: csgo.INT, Enc: csgo.NOCOMP}
	availqty := csgo.AttrInfo{Name: "ps_availqty", Type: csgo.INT, Enc: csgo.NOCOMP}
	supplycost := csgo.AttrInfo{Name: "ps_supplycost", Type: csgo.FLOAT, Enc: csgo.NOCOMP}
	comment := csgo.AttrInfo{Name: "ps_comment", Type: csgo.STRING, Enc: csgo.NOCOMP}

	sig = append(sig, partkey, suppkey, availqty, supplycost, comment)
	
	table := cs.CreateRelation("partsupp", sig)
	table.Load("C:/Uni/Projekte/Ausgabe/partsupptest.tbl", '|')
	table = table.Scan([]csgo.AttrInfo{partkey, suppkey, availqty, supplycost})
	table = table.Select(availqty, csgo.GT, 1000)
	table.Print()*/
	//for the new
	var cs csgo.ColumnStorer
	cs = csgo.NewColumnStore()
	var sig []csgo.AttrInfo

	partkey := csgo.AttrInfo{Name: "ps_partkey", Type: csgo.INT, Enc: csgo.NOCOMP}
	suppkey := csgo.AttrInfo{Name: "ps_suppkey", Type: csgo.INT, Enc: csgo.NOCOMP}
	availqty := csgo.AttrInfo{Name: "ps_availqty", Type: csgo.INT, Enc: csgo.NOCOMP}
	supplycost := csgo.AttrInfo{Name: "ps_supplycost", Type: csgo.FLOAT, Enc: csgo.NOCOMP}
	comment := csgo.AttrInfo{Name: "ps_comment", Type: csgo.STRING, Enc: csgo.NOCOMP}

	sig = append(sig, partkey, suppkey, availqty, supplycost, comment)

	table := cs.CreateRelation("partsupptest", sig)
	table.Load("C:/Uni/Projekte/DBImpl-projekt/Eingabe/partsupptest.tbl", '|')
	table = table.Scan([]csgo.AttrInfo{partkey, suppkey, availqty, supplycost})
	table = table.Select(availqty, csgo.GT, 3000)
	table.Print()
	table.GetRawData()
}
