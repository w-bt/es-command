package command

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var (
	routingIDS = []string{"60234", "79483", "122583", "102770", "135178", "72116", "65480", "88652", "12347", "89982", "71079", "64699", "59612", "111823", "139049", "83852", "58018", "32153", "49335", "444", "57673", "93545", "33328", "52580", "52568", "24428", "74324", "147522", "68757", "55518", "149504", "47588", "142530", "133873", "89381", "122584", "69814", "82296", "83621", "137700", "7179", "122631", "79074", "99687", "77587", "3850", "114475", "81514", "121089", "114828", "90339", "121023", "66405", "72164", "63179", "139857", "83552", "107241", "72136", "5364", "122499", "61491", "42018", "52570", "85166", "65633", "122204", "131687", "142007", "68766", "57936", "89525", "7049", "109409", "24819", "47648", "29299", "17460", "65417", "69125", "82096", "46647", "144236", "84181", "40837", "24361", "52573", "38521", "92828", "96328", "17099", "67624", "24818", "46193", "86878", "65422", "88120", "49929", "49780", "74403", "76747", "107945", "24527", "65723", "68575", "24359", "18647", "9221", "149915", "65933", "68207", "116880", "75415", "77461", "92650", "93050", "18646", "100725", "142963", "149678", "3646", "67475", "40297", "11052", "117778", "77144", "4094", "87034", "108719", "30517", "84287", "135188", "58088", "46941", "78651", "5987", "92541", "131694", "144082", "144968", "90238", "151482", "24378", "101684", "39838", "83284", "30838", "10938", "10437", "124691", "149152", "91268", "49326", "134228", "138675", "121698", "64015", "14293", "19017", "75368", "24537", "135137", "147908", "97953", "94110", "24514", "122149", "59767", "87321", "132989", "47727", "130933", "45851", "127384", "81116", "68820", "65221", "133870", "52359", "65239", "65285", "147580", "138376", "68758", "52956", "122084", "92151", "70040", "71782", "150996", "14292", "1430", "109724", "103884", "148782", "127626", "95149", "63635", "107628", "144063", "4386", "47320", "118654", "66688", "71029", "74446", "99091", "147014", "79451", "42508", "150998", "54177", "70435", "146148", "92150", "63441", "15414", "24538", "64493", "47253", "61572", "140088", "18430", "5986", "57743", "19778", "95518", "122118", "135169", "69081", "24518", "133062", "132432", "14151", "148477", "92516", "21040", "66829", "126539", "56074", "71255", "5424", "70699", "8878", "144387", "7980", "8706", "83353", "24414", "85278", "107035", "90665", "90899", "109265", "124770", "135174", "145755", "10121", "78893", "65656", "48264", "64158", "150837", "60378", "48853", "67447", "93280", "24515", "60384", "86029", "140121", "13290", "65513", "79568", "122158", "145081", "59171", "141125", "124904", "47988", "147467", "66773", "141413", "79058", "13059", "68259", "90902", "60855", "24369", "145055", "147619", "97756", "65332", "77413", "67450", "73055", "86892", "138672", "88205", "115246", "133789", "109792", "132500", "89843", "94319", "32170", "79076", "86538", "134034", "135763", "7218", "24313", "19686", "8918", "116860", "93473", "52578", "99047", "112742", "43717", "146961", "100191", "83512", "15754", "79004", "48149", "78311", "108176", "34631", "114615", "110125", "24582", "94133", "79503", "97606", "24815", "122594", "149901", "63397", "69876", "129514", "13541", "9155", "68569", "49327", "63973", "97434", "68457", "16000", "67778", "56259", "68563", "25364", "24309", "5894", "12495", "132493", "8786", "45967", "63081", "92152", "49323", "86239", "71986", "40657", "134044", "132437", "98716", "94317", "121777", "150009", "125503", "3", "130038", "42322", "63962", "121524", "29713", "37206", "12256", "32609", "93028", "79507", "84501", "124905", "91658", "65662", "63043", "33186", "110306", "146944", "90695", "67157", "31199", "62606", "66453", "64702", "40674", "122172", "46761", "61527", "62289", "138733", "150520", "61782", "70038", "8696", "65686"}
	indices    = []string{
		"crm_tasks_production_20221219000000000",
		"crm_deals_production_20230111233000000",
		"crm_geolocations_production_20221219000000000",
		"crm_people_production_20231121133000001",
	}
	prevTask = map[int][]string{
		0: []string{"K07XDIo2TaO31UWIsZxYOw", "66oYwxVVTAeopnZnkqPzrQ"},
		1: []string{"iTZsd-AATpmGI8NcRUZjkA", "DUPF2Oe5S2aAtlCrodhWSA"},
		2: []string{"qGJX2QmcTaupZjyGgVVZiQ", "ath_8I8fShSvsTACENyoLg"},
		3: []string{"A9BfAS-FTGu0g0chdmlnjw", "LjLqn-TZT1mCoadSwy2eJw"},
		4: []string{"rA5ld0wIRV6AMYomAyiu_Q", "ZhRbg3zvQqedaRIeGrTUAg"},
		5: []string{"kwgeUUSISLeBBrofU8J0JA", "6tAODouFSOuIJsdL2u1ulQ"},
	}
	prevDeal = map[int][]string{
		0: []string{"LjLqn-TZT1mCoadSwy2eJw", "66oYwxVVTAeopnZnkqPzrQ"},
		1: []string{"K07XDIo2TaO31UWIsZxYOw", "ath_8I8fShSvsTACENyoLg"},
		2: []string{"rA5ld0wIRV6AMYomAyiu_Q", "iTZsd-AATpmGI8NcRUZjkA"},
		3: []string{"kwgeUUSISLeBBrofU8J0JA", "6tAODouFSOuIJsdL2u1ulQ"},
		4: []string{"qGJX2QmcTaupZjyGgVVZiQ", "DUPF2Oe5S2aAtlCrodhWSA"},
		5: []string{"A9BfAS-FTGu0g0chdmlnjw", "ZhRbg3zvQqedaRIeGrTUAg"},
	}
	prevGeo = map[int][]string{
		0: []string{"ZhRbg3zvQqedaRIeGrTUAg", "ath_8I8fShSvsTACENyoLg"},
		1: []string{"K07XDIo2TaO31UWIsZxYOw", "iTZsd-AATpmGI8NcRUZjkA"},
		2: []string{"qGJX2QmcTaupZjyGgVVZiQ", "66oYwxVVTAeopnZnkqPzrQ"},
		3: []string{"A9BfAS-FTGu0g0chdmlnjw", "6tAODouFSOuIJsdL2u1ulQ"},
		4: []string{"kwgeUUSISLeBBrofU8J0JA", "DUPF2Oe5S2aAtlCrodhWSA"},
		5: []string{"rA5ld0wIRV6AMYomAyiu_Q", "LjLqn-TZT1mCoadSwy2eJw"},
	}
	prevPerson = map[int][]string{
		0: []string{"K07XDIo2TaO31UWIsZxYOw", "66oYwxVVTAeopnZnkqPzrQ"},
		1: []string{"LjLqn-TZT1mCoadSwy2eJw", "ath_8I8fShSvsTACENyoLg"},
		2: []string{"rA5ld0wIRV6AMYomAyiu_Q", "iTZsd-AATpmGI8NcRUZjkA"},
		3: []string{"kwgeUUSISLeBBrofU8J0JA", "6tAODouFSOuIJsdL2u1ulQ"},
		4: []string{"A9BfAS-FTGu0g0chdmlnjw", "qGJX2QmcTaupZjyGgVVZiQ"},
		5: []string{"ZhRbg3zvQqedaRIeGrTUAg", "DUPF2Oe5S2aAtlCrodhWSA"},
	}
)

type ShardResponse struct {
	Node  string `json:"node"`
	Shard int    `json:"shard"`
}

type SearchShardResponse struct {
	Shards [][]ShardResponse `json:"shards"`
}

func CheckRoutingShards() {
	// Open the CSV file for writing
	fileOutput, err := os.Create("output_shard_distribution.csv")
	if err != nil {
		panic(err)
	}
	defer fileOutput.Close()

	// Create a CSV writer
	writer := csv.NewWriter(fileOutput)
	defer writer.Flush()

	// Write header
	header := []string{"index", "routing", "shard", "first_node", "second_node"}

	err = writer.Write(header)
	if err != nil {
		panic(err)
	}

	// looping per indices
	for _, indexName := range indices {
		// looping per routing
		for _, routing := range routingIDS {
			var row []string
			row = append(row, indexName)
			log.Printf("checking index %s for routing %s", indexName, routing)
			nodes, index := getNodeAllocations(indexName, routing)
			row = append(row, routing)
			row = append(row, fmt.Sprintf("%d", index))
			row = append(row, nodes...)
			err := writer.Write(row)
			if err != nil {
				panic(err)
			}
		}
	}

	// Flush the writer to ensure all data is written to the file
	writer.Flush()
}

func getNodeAllocations(indexName, routing string) ([]string, int) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/_search_shards?routing=%s", sourceHostName, indexName, routing), nil)
	if err != nil {
		return []string{}, 0
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	rawResp, err := client.Do(req)
	if err != nil {
		return []string{}, 0
	}
	defer rawResp.Body.Close()

	bodyResult, err := ioutil.ReadAll(rawResp.Body)
	if err != nil {
		return []string{}, 0
	}

	var resp SearchShardResponse
	err = json.Unmarshal(bodyResult, &resp)
	if err != nil {
		log.Printf("%+v", err)
		return []string{}, 0
	}

	var result []string
	var index int
	for _, item := range resp.Shards[0] {
		index = item.Shard
	}

	switch indexName {
	case "crm_tasks_production_20221219000000000":
		result = prevTask[index]
	case "crm_deals_production_20230111233000000":
		result = prevDeal[index]
	case "crm_geolocations_production_20221219000000000":
		result = prevGeo[index]
	case "crm_people_production_20231121133000001":
		result = prevPerson[index]
	}

	return result, index
}
