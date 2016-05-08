package main


//import "C:/Uni/Projekte/DBImpl-projekt/interfaces.go"
import "csgo"
//import "fmt"

 
 
func main() {
	var name string
	var sig []csgo.AttrInfo
	var cs csgo.ColumnStore
	
	cs.Relations = make(map[string]csgo.Relation)
	name = "NewTable"
	cs.CreateRelation( name, sig )
    cs.Relations[name] = cs.Relations[name].Load( "C:/Uni/Projekte/Ausgabe/nation.tbl", 0x7C )
	cs.Relations[name].Print()
}
