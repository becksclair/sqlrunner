package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	_ "github.com/lib/pq"
)

// AppConfig holds json structure for the application configuration
type AppConfig struct {
	ConnString string `json:"postgres_connection"`
}

// Define our global application config
var appConfig AppConfig

func main() {
	fmt.Println("SQL Runner")

	// Setup file logging
	f, err := os.OpenFile("runner.log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer CloseFile(f)
	log.SetOutput(f)

	// Connect to the database
	LoadSettings()
	db := DBConnect()
	err = db.Ping()
	if err != nil {
		log.Fatal("Error: Could not establish a connection with the database")
	}

	log.Println("Executing SQL commands...")
	sqlCommands := LoadSQLCommands()
	ExecuteSQLCommands(db, sqlCommands)

	log.Println("SQL Commands executed.")
}

// LoadSettings parses the settings json file and sets it up for us
//noinspection SpellCheckingInspection
func LoadSettings() {
	jsonFile, err := os.Open("sqlrunner.json")
	if err != nil {
		log.Fatalf("Error loading settings %s", err)
	}
	defer CloseFile(jsonFile)

	// Read our json file as a byte array
	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Deserialize our json file into our structure
	err = json.Unmarshal(byteValue, &appConfig)
	CheckErr(err)

	// Prepend disable Postgres SSL mode to the connection string
	appConfig.ConnString = "sslmode=disable " + appConfig.ConnString
}

// LoadSQLCommands reads the SQL commands to execute from the file
// and returns a string buffer with the commands
func LoadSQLCommands() string {
	sqlFile, err := os.Open("commands.sql")
	if err != nil {
		log.Fatal("Error loading SQL commands file, check 'commands.sql': ", err)
	}
	defer CloseFile(sqlFile)

	byteValue, _ := ioutil.ReadAll(sqlFile)
	sqlCommands := string(byteValue)
	return sqlCommands
}

// DBConnect connects to a database using the application configuration
// connection string and returns a pointer to the connection.
func DBConnect() *sql.DB {
	db, err := sql.Open("postgres", appConfig.ConnString)
	CheckErr(err)
	return db
}

// ExecuteSQLCommands executes the commands in the buffer "sqlCommands"
func ExecuteSQLCommands(db *sql.DB, sqlCommands string) {
	_, err := db.Exec(sqlCommands)
	CheckErr(err)

	// Clean resources and finish
	log.Println("Done")
	err = db.Close()
	CheckErr(err)
}

// CloseFile closes the argument file and checks for errors
func CloseFile(f *os.File) {
	err := f.Close()
	CheckErr(err)
}

// CheckErr checks the argument error and logs it
func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
