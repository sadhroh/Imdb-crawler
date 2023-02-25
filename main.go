/*
 *-----------------------------------------------------------------
 * IMDb Chart Fetcher
 *-----------------------------------------------------------------
 * Description: Program takes a particular IMDb URL along with the
 *              maximum number of records required & provides the
 *              JSON string of the obtained list of movies from the
 *              IMDb website.
 *              The following details of the movies are fetched:
 *               - title
 *               - movie release year
 *               - imdb rating
 *               - summary
 *               - duration
 *               - genre
 *              The program utilizes the concept of Web scraping &
 *              Web Crawling to get the movie details from the URL.
 *
 * Programming Language: Golang [version go1.15.3 linux/amd64]
 *
 * Development Environment:
 * DISTRIB_ID=Ubuntu
 * DISTRIB_RELEASE=20.04
 * DISTRIB_CODENAME=focal
 * DISTRIB_DESCRIPTION="Ubuntu 20.04.1 LTS"
 *
 * Usage:
 * ./imdb_chart_fetcher 'chart_url' items_count
 * where
 *  - items_count is the number of movies needed
 *  - chart_url is the IMDb URL to fetch the data from
 *  - imdb_chart_fetcher is the binary
 *
 * The binary, imdb_chart_fetcher should be present but it is highly
 * recommended that the binary be created for the system on which it
 * is to be executed.
 *
 * To create the binary:
 *  - Navigate to the folder containing source code [main.go] file
 *    Make sure the GOPATH is set to point to the workspace where
 *    this program is kept.
 *  - Enter the line:
 *    go build -o imdb_chart_fetcher .
 *  - This should create the executable binary in the current folder
 *
 *-----------------------------------------------------------------
 */
package main

// NO external frameworks/packages are used. Packages already present in golang v1.15.3 are used
import (
    "os"
    "fmt"
    "log"
    "sync"
    "regexp"
    "strings"
    "strconv"
    "net/http"
    "io/ioutil"
    "encoding/json"
)

// IMDB URL constants for web crawling/scraping
const (
    imdb_url_Main    = `https://www.imdb.com`
    chart_url_Indian = `https://www.imdb.com/india/top-rated-indian-movies`
    chart_url_Tamil  = `https://www.imdb.com/india/top-rated-tamil-movies`
    chart_url_Telugu = `https://www.imdb.com/india/top-rated-telugu-movies`
)

// HTML element classes used as selectors to find the element
const (
    td_titleClass     = `titleColumn`
    td_ratingClass    = `ratingColumn imdbRating`
    releaseYear_class = `secondaryInfo`
    summary_class     = `summary_text`
)

// field separator as present in IMDB for separating multiple data
const (
    field_separator = `<span class="ghost">|</span>`
)

// Structure to maintain the summary, duration & genre
// facilitates easy conversion from structure to json by using the meta-fields
type MovDetail struct {
    Summary  string `json:"summary"`
    Duration string `json:"duration"`
    Genre    string `json:"genre"`
}

// Structure to maintain the title, release year as well as movie details like
// summary, duration & genre via embedding the MovDetail structure.
// facilitates easy conversion from structure to json by using the meta-fields
// as the emebedded structure meta fields are also taken as is.
type TitleData struct {
    Title       string `json:"title"`
    ReleaseYear uint64 `json:"movie_release_year"`
    MovDetail
}

// The overall chart data which specifies the TitleData, via embedding as well
// as the rating that is obtained separately.
// facilitates easy conversion from structure to json by using the meta-fields
// as the emebedded structure meta fields are also taken as is.
type ImdbChartData struct {
    TitleData
    Rating      float64 `json:"imdb_rating"`
}

// crawlForMoreInfo is a web crawler to fetch the duration, genre & summary via using
// the link provided in the main movie table.
// This function is triggered as a goroutine to process concurrently while other data
// is being fetched/populated.
func crawlForMoreInfo (cUrl string, crawlChan chan<- MovDetail){

    var wg sync.WaitGroup

    resp, err := http.Get (cUrl)
    if err != nil{
        log.Println ("FAILURE: Failed to establish GET request for more info")
    }
    if resp.StatusCode != http.StatusOK {
        log.Println ("FAILURE: Cannot process response. Response Code:", resp.StatusCode)
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil{
        log.Println ("ERROR: Failed to obtain response body for more info")
    }
    if err != nil {
        crawlChan<- MovDetail{}
    }
    respBody := string(body)

    // duration
    durEndIdx := strings.Index(respBody, `</time>`)
    durStrtIdx := strings.LastIndex(respBody[ : durEndIdx], `>`) + 1

    // summary
    summaryDivAttr := `<div class="`+summary_class+`">`
    summaryStrtIdx := strings.Index(respBody, summaryDivAttr) + len (summaryDivAttr)
    summaryEndIdx := strings.Index(respBody[summaryStrtIdx : ], `</div>`) + summaryStrtIdx
    summaryData := []byte(strings.TrimSpace(respBody[summaryStrtIdx : summaryEndIdx]))

    // check if the summary is not complete and a link to the full summary is given
    if newLnk := strings.Index (string(summaryData), `<a href="`); newLnk != -1 {
	    newLnkEndIdx := newLnk + strings.Index(string(summaryData[newLnk + len (`<a href="`) : ]), `>`)
	    fullSummaryUrl := imdb_url_Main + string(summaryData[newLnk + len (`<a href="`) : newLnkEndIdx])

	    wg.Add(1)

	    // let the goroutine extract the full summary using the URL for the same
	    go func (){
                defer wg.Done()

		resp, err := http.Get (fullSummaryUrl)
		if err != nil{
			log.Println ("FAILURE: Failed to establish GET request for more info")
		}
		if resp.StatusCode != http.StatusOK {
			log.Println ("FAILURE: Cannot process response. Response Code:", resp.StatusCode)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil{
			log.Println ("ERROR: Failed to obtain response body for more info")
		}
		if err != nil {
			crawlChan<- MovDetail{}
		}
		respBody := string(body)

		// expanded summary
		summaryData = []byte(respBody[strings.Index(respBody, `<p>`) + len (`<p>`) : strings.Index(respBody, `</p>`)])
	    }()
    }

    // genre
    genreSecStrtIdx := strings.Index(respBody[durEndIdx : ], field_separator) + durEndIdx + len (field_separator)
    genreSecEndIdx := strings.Index(respBody[genreSecStrtIdx : ], field_separator) + genreSecStrtIdx

    // the movie can be of multiple genres, each having a <a> HTML element
    // filetering out & splitting using regexp
    r := regexp.MustCompile (`</a>`)
    genreCatLnks := r.Split(respBody[genreSecStrtIdx : genreSecEndIdx], -1)

    genreLst := []string {}

    // create a slice of genres and later join them
    // better than creating multiple strings by concatenation
    for _, v := range genreCatLnks {
        genreCatIdx := strings.LastIndex(v, `>`)
        if genreCatIdx == -1 {
            continue
        }
        genreCatIdx++
        genreLst = append (genreLst, v[genreCatIdx : ])
    }

    wg.Wait()

    // send the details via the channel to signal other goroutines of its completion
    crawlChan<- MovDetail{
	    string(summaryData),
            strings.TrimSpace(respBody[durStrtIdx : durEndIdx]),
            strings.Join(genreLst, ", "),
        }

}

// getTitleData is triggered as a goroutine and it fetches & parses the data from
// the IMDb row of the table. The function triggers the crawler as a goroutine with
// relevant parameters to obtain the summary, genre & duration while it processes
// other data present in the field like Movie title & release date.
func getTitleData (movieRec string, t *TitleData, wg *sync.WaitGroup) {

    defer wg.Done()

    // title data
    // contains title, release year, and link to summary, duration & genre
    tdtitleAttr := `<td class="`+td_titleClass+`">`
    titleStrtIdx := strings.Index(movieRec, tdtitleAttr) + len (tdtitleAttr)
    titleEndIdx := strings.Index(movieRec[titleStrtIdx : ], `</td>`) + titleStrtIdx

    // link to more info
    moreInfoAttr := `<a href="`
    urlStrtIdx := titleStrtIdx + strings.Index(movieRec[titleStrtIdx : titleEndIdx], moreInfoAttr) + len (moreInfoAttr)
    urlEndIdx := urlStrtIdx + strings.Index(movieRec[urlStrtIdx : titleEndIdx], `"`)
    moreInfoURL := imdb_url_Main + movieRec[urlStrtIdx : urlEndIdx]

    // start crawler to fetch summary, duration & genre concurrently
    crawlChan := make (chan MovDetail)
    defer close (crawlChan)
    go crawlForMoreInfo (moreInfoURL, crawlChan)

    // only title
    title := movieRec[titleStrtIdx + strings.Index(movieRec[titleStrtIdx : titleEndIdx], `>`) + 1 :
                      titleStrtIdx + strings.LastIndex(movieRec[titleStrtIdx : titleEndIdx], `</a>`)]
    t.Title = title

    // release date
    releaseDateAttr := `<span class="`+releaseYear_class+`">`
    releaseYear := movieRec[titleStrtIdx + strings.Index(movieRec[titleStrtIdx : titleEndIdx], releaseDateAttr) + len (releaseDateAttr) + 1 :
                            titleStrtIdx + strings.LastIndex(movieRec[titleStrtIdx : titleEndIdx], `</span>`) - 1]
    year, err := strconv.ParseUint(releaseYear, 10, 64)
    if err != nil {
        log.Println ("FAILURE: Could not obtain release year for", title)
    }
    t.ReleaseYear = year

    // wait for the crawler to fetch the data and populate the structure
    t.MovDetail = <-crawlChan
}

// getRating handles the extraction of rating from the specific row for that movie.
// As this is triggered as a goroutine, it processes the rating and populates the
// correct field supplied concurrently.
func getRating (movieRec string, rate *float64, wg *sync.WaitGroup) {

    defer wg.Done()

    // rating
    tdRatingAttr := `<td class="`+td_ratingClass+`">`
    ratingStrtIdx := strings.Index(movieRec, tdRatingAttr) + len (tdRatingAttr)
    ratingEndIdx := strings.Index(movieRec[ratingStrtIdx : ], `</td>`) + ratingStrtIdx

    rating := movieRec[ratingStrtIdx + strings.Index(movieRec[ratingStrtIdx : ratingEndIdx], `>`) + 1 :
                       ratingStrtIdx + strings.LastIndex (movieRec[ratingStrtIdx : ratingEndIdx], `</strong>`)]
    imdbRate,err := strconv.ParseFloat(rating, 64)
    if err != nil {
        log.Println ("FAILURE: Could not obtain rating")
    }
    *rate = imdbRate
}

// parseTableData is the master that is responsible for trigerring the proper
// goroutine and synchronizing them, all while parsing the given data as per the
// IMDb website.
// The rows, for the specific movie, is split and processed. Then end result is
// the requested number of records or the maximum number of records currently
// available for that category.
// When all the movies are processed, they are dumped as JSON string.
func parseTableData(table string, item_count int, parserChan chan<- string) {

    var wg sync.WaitGroup

    r := regexp.MustCompile (`<tr>*`)

    recSlc := r.Split(table, -1)
    recSlc = recSlc[2:]

    if (item_count > len (recSlc)){
        log.Printf ("ALARM: Only %d records available\n", len (recSlc))
	item_count = len (recSlc)
    }

    imdbChartTable := make([]ImdbChartData, item_count)

    for i, mov := range recSlc {
        if (i == item_count) {
            break
        }
        wg.Add(2)
        go getTitleData (mov, &imdbChartTable[i].TitleData, &wg)
        go getRating (mov, &imdbChartTable[i].Rating, &wg)
    }

    // wait for the goroutines to complete populating the fields
    wg.Wait()

    // convert the data in the structure to JSON format
    imdbChart, err := json.Marshal (imdbChartTable)
    if err != nil {
        log.Fatal ("ERROR: Unable to parse records", err)
    }

    // send the data back to the caller
    parserChan<- string(imdbChart)
}

// validateUrl just checks if the URL given as command-line is one of the URLs configured.
func validateUrl () string {
    switch os.Args[1]{
    case chart_url_Indian, chart_url_Telugu, chart_url_Tamil: return os.Args[1]
    default: log.Fatal ("Invalid URL")
    }
    return ""
}

func main(){
    // check if proper arguments are provided
    if len (os.Args) < 3 {
        log.Fatal ("Please provide the URL and the total count of movies")
    }

    chart_url := validateUrl()
    item_count, err := strconv.Atoi (os.Args[2])
    if err != nil {
        log.Fatal ("ERROR:", err)
    }

    // Obtain the IMDb result body via http GET request
    resp, err := http.Get (chart_url)
    if err != nil{
        log.Fatal ("ERROR: Failed to establish GET request")
    }
    if resp.StatusCode != http.StatusOK {
        log.Fatal ("ERROR: Cannot process response. Response Code:", resp.StatusCode)
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil{
        log.Fatal ("ERROR: Failed to obtain response body")
    }

    // only extract the table containing the movie list
    tableStrtIdx := strings.Index(string(body), "<table")
    tableEndIdx := strings.Index(string(body), "</table>")
    table := string(body)[tableStrtIdx : tableEndIdx + len ("</table>")]

    // Start the master goroutine to parse the table and provide JSON dump
    parserChan := make (chan string)
    defer close (parserChan)
    go parseTableData (table, item_count, parserChan)

    fmt.Println (<-parserChan)
}
