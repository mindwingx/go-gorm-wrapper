package sqlwrapper

import (
	SdkSql "database/sql"
	"fmt"
	"github.com/fatih/color"
	"github.com/mindwingx/abstraction"
	"github.com/mindwingx/go-helper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"time"
)

type (
	sql struct {
		config dbConfig
		locale abstraction.Locale
		db     *gorm.DB
	}

	dbConfig struct {
		Debug              bool
		Host               string
		Port               string
		Username           string
		Password           string
		Database           string
		Ssl                string
		MaxIdleConnections int
		MaxOpenConnections int
		MaxLifetimeSeconds int
		SlowSqlThreshold   int
	}
)

func NewSql(registry abstraction.Registry, locale abstraction.Locale) abstraction.Sql {
	database := new(sql)
	err := registry.Parse(&database.config)
	if err != nil {
		helper.CustomPanic("", err)
	}

	database.locale = locale

	return database
}

func (g *sql) InitSql() {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=%s",
		g.config.Host,
		g.config.Username,
		g.config.Password,
		g.config.Database,
		g.config.Port,
		g.config.Ssl,
	)

	database, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 g.newGormLog(g.config.SlowSqlThreshold),
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
	})
	if err != nil {
		helper.CustomPanic(g.locale.Get("sql_open_conn_err"), err)
	}

	sqlDatabase, err := database.DB()
	if err != nil {
		helper.CustomPanic(g.locale.Get("sql_retrieve_conn_err"), err)
	}

	if g.config.MaxIdleConnections != 0 {
		sqlDatabase.SetMaxIdleConns(g.config.MaxIdleConnections)
	}

	if g.config.MaxOpenConnections != 0 {
		sqlDatabase.SetMaxOpenConns(g.config.MaxOpenConnections)
	}

	if g.config.MaxLifetimeSeconds != 0 {
		sqlDatabase.SetConnMaxLifetime(time.Second * time.Duration(g.config.MaxLifetimeSeconds))
	}

	if g.config.Debug {
		database = database.Debug()
		color.Yellow(g.locale.Get("sql_debug_enable"))
	}

	g.db = database
}

// Migrate migration files base path
func (g *sql) Migrate(path string) {
	//path := fmt.Sprintf("%s/database/postgres", src.RootPath())

	// Open the directory
	dir, err := os.Open(path)
	if err != nil {
		helper.CustomPanic(g.locale.Get("sql_scan_sql_dir_err"), err)
		return
	}

	defer dir.Close()

	// Read the directory contents
	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		fmt.Println(g.locale.Get("sql_dir_read_err"), err)
		return
	}

	// Sort the entries alphabetically by name - sql file order by numeric(01, 02, etc)
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].Name() < fileInfos[j].Name()
	})

	// Iterate over the file info slice and print the file names
	for _, fileInfo := range fileInfos {
		if fileInfo.Mode().IsRegular() {
			if err = g.db.Exec(g.parseSqlFile(path, fileInfo)).Error; err != nil {
				helper.CustomPanic(g.locale.Get("sql_migrate_err"), err)
			}
		}
	}
}

// Queries

func (g *sql) Close() {
	sqlDatabase, err := g.db.DB()
	if err != nil {
		helper.CustomPanic(g.locale.Get("sql_close_conn_err"), err)
	}

	err = sqlDatabase.Close()
	if err != nil {
		helper.CustomPanic(g.locale.Get("sql_close_conn_err"), err)
	}
}

func (g *sql) Where(query interface{}, args ...interface{}) abstraction.Query {
	return g.db.Where(query, args...)
}

func (g *sql) Or(query interface{}, args ...interface{}) abstraction.Query {
	return g.db.Or(query, args...)
}

func (g *sql) Not(query interface{}, args ...interface{}) abstraction.Query {
	return g.db.Not(query, args...)
}

func (g *sql) Limit(value int) abstraction.Query {
	return g.db.Limit(value)
}

func (g *sql) Offset(value int) abstraction.Query {
	return g.db.Offset(value)
}

func (g *sql) Order(value string) abstraction.Query {
	return g.db.Order(value)
}

func (g *sql) Select(query interface{}, args ...interface{}) abstraction.Query {
	return g.db.Select(query, args...)
}

func (g *sql) Omit(columns ...string) abstraction.Query {
	return g.db.Omit(columns...)
}

func (g *sql) Group(query string) abstraction.Query {
	return g.db.Group(query)
}

func (g *sql) Having(query string, values ...interface{}) abstraction.Query {
	return g.db.Having(query, values...)
}

func (g *sql) Joins(query string, args ...interface{}) abstraction.Query {
	return g.db.Joins(query, args...)
}

func (g *sql) Scopes(funcs ...func(abstraction.Query) abstraction.Query) abstraction.Query {
	var scopes []func(*gorm.DB) *gorm.DB

	for _, f := range funcs {
		// Dereference the pointer to *gorm.DB and convert it to *gorm.DB
		scopes = append(scopes, func(db *gorm.DB) *gorm.DB {
			return f(db) // in fact: f(abstraction.Query(db))
		})
	}

	return g.db.Scopes(scopes...)
}

func (g *sql) Unscoped() abstraction.Query {
	return g.db.Unscoped()
}

func (g *sql) Attrs(attrs ...interface{}) abstraction.Query {
	return g.db.Attrs(attrs...)
}

func (g *sql) Assign(attrs ...interface{}) abstraction.Query {
	return g.db.Assign(attrs...)
}

func (g *sql) First(out interface{}, where ...interface{}) abstraction.Query {
	return g.db.First(out, where...)
}

func (g *sql) Last(out interface{}, where ...interface{}) abstraction.Query {
	return g.db.Last(out, where...)
}

func (g *sql) Find(out interface{}, where ...interface{}) abstraction.Query {
	return g.db.Find(out, where...)
}

func (g *sql) Scan(dest interface{}) abstraction.Query {
	return g.db.Scan(dest)
}

func (g *sql) Row() *SdkSql.Row {
	return g.db.Row()
}

func (g *sql) Rows() (*SdkSql.Rows, error) {
	return g.db.Rows()
}

func (g *sql) ScanRows(rows *SdkSql.Rows, result interface{}) error {
	return g.db.ScanRows(rows, result)
}

func (g *sql) Pluck(column string, value interface{}) abstraction.Query {
	return g.db.Pluck(column, value)
}

func (g *sql) Count(value *int64) abstraction.Query {
	return g.db.Count(value)
}

func (g *sql) FirstOrInit(out interface{}, where ...interface{}) abstraction.Query {
	return g.db.FirstOrInit(out, where...)
}

func (g *sql) FirstOrCreate(out interface{}, where ...interface{}) abstraction.Query {
	return g.db.FirstOrCreate(out, where...)
}

func (g *sql) Update(column string, attrs ...interface{}) abstraction.Query {
	return g.db.Update(column, attrs)
}

func (g *sql) Updates(values interface{}) abstraction.Query {
	return g.db.Updates(values)
}

func (g *sql) UpdateColumn(column string, attrs ...interface{}) abstraction.Query {
	return g.db.UpdateColumn(column, attrs)
}

func (g *sql) UpdateColumns(values interface{}) abstraction.Query {
	return g.db.UpdateColumns(values)
}

func (g *sql) Save(value interface{}) abstraction.Query {
	return g.db.Save(value)
}

func (g *sql) Create(value interface{}) abstraction.Query {
	return g.db.Create(value)
}

func (g *sql) Delete(value interface{}, where ...interface{}) abstraction.Query {
	return g.db.Delete(value, where...)
}

func (g *sql) Raw(sql string, values ...interface{}) abstraction.Query {
	return g.db.Raw(sql, values...)
}

func (g *sql) Exec(sql string, values ...interface{}) abstraction.Query {
	return g.db.Exec(sql, values...)
}

func (g *sql) Model(value interface{}) abstraction.Query {
	return g.db.Model(value)
}

func (g *sql) Table(name string) abstraction.Query {
	return g.db.Table(name)
}

func (g *sql) Debug() abstraction.Query {
	return g.db.Debug()
}

func (g *sql) Begin() abstraction.Query {
	return g.db.Begin()
}

func (g *sql) Commit() abstraction.Query {
	return g.db.Commit()
}

func (g *sql) Rollback() abstraction.Query {
	return g.db.Rollback()
}

func (g *sql) AutoMigrate(values ...interface{}) error {
	return g.db.AutoMigrate(values...)
}

func (g *sql) Association(column string) *gorm.Association {
	return g.db.Association(column)
}

func (g *sql) Preload(column string, conditions ...interface{}) abstraction.Query {
	return g.db.Preload(column, conditions...)
}

func (g *sql) Set(name string, value interface{}) abstraction.Query {
	return g.db.Set(name, value)
}

func (g *sql) InstanceSet(name string, value interface{}) abstraction.Query {
	return g.db.InstanceSet(name, value)
}

func (g *sql) Get(name string) (interface{}, bool) {
	return g.db.Get(name)
}

func (g *sql) SetJoinTable(model interface{}, column string, handler interface{}) error {
	err := g.db.SetupJoinTable(model, column, handler)
	if err != nil {
		return err
	}

	return nil
}

func (g *sql) AddError(err error) error {
	return g.db.AddError(err)
}

func (g *sql) RowsAffected() int64 {
	return g.db.RowsAffected
}

func (g *sql) Error() error {
	return g.db.Error
}

// HELPER METHODS

func (g *sql) newGormLog(SlowSqlThreshold int) logger.Interface {
	return logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Duration(SlowSqlThreshold) * time.Second, // Slow SQL threshold
			LogLevel:                  logger.Warn,                                   // Log level
			IgnoreRecordNotFoundError: false,                                         // Ignore ErrRecordNotFound error for logger
			Colorful:                  true,                                          // Disable color
		})
}

func (g *sql) parseSqlFile(path string, fileInfo os.FileInfo) string {
	sqlFile := fmt.Sprintf("%s/%s", path, fileInfo.Name())
	sqlBytes, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		helper.CustomPanic(g.locale.Get("sql_failed_to_parse_sql"), err)
	}
	// Convert SQL file contents to string
	q := string(sqlBytes)
	return q
}

func (g *sql) Seed() {
	/*var count int64
	result := g.db.Model(&domain.User{}).Count(&count)

	if result.Error != nil {
		helper.CustomPanic(g.locale.Get("sql_seed_inquire_err"), result.Error)
	}

	if count == 0 {
		color.Yellow(g.locale.Get("sql_seed_start"))

		for _, user := range g.mockedPayload() {
			res := g.db.Create(&user)
			if res.Error != nil {
				helper.CustomPanic(g.locale.Get("sql_seed_fail"), res.Error)
			}
		}

		color.Yellow(g.locale.Get("sql_seed_finished"))
	}*/
}

/*func (g *sql) mockedPayload() []domain.User {
	var mockedUsers []domain.User

	mockedUsers = append(
		mockedUsers,
		domain.User{
			Username:  "jimmy",
			FirstName: "Jim",
			LastName:  "Karry",
			Phone:     "989352231232",
			Email:     "jimmy@mejib.io",
			Password:  "password",
			Active:    true,
		},
		domain.User{
			Username:  "katty",
			FirstName: "Katty",
			LastName:  "Harold",
			Phone:     "989125693625",
			Email:     "katty@mejib.io",
			Password:  "password",
			Active:    true,
		},
		domain.User{
			Username:  "moer",
			FirstName: "Moer",
			LastName:  "Pure",
			Phone:     "989113695899",
			Email:     "moer@mejib.io",
			Password:  "password",
			Active:    true,
		},
	)

	return mockedUsers
}*/
