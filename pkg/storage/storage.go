// Пакет для работы с БД приложения GoNews.
package storage

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

const (
	username = "root"
	password = "root"
	hostname = "127.0.0.1"
	port     = 3306
	dbName   = "newsdb"
)

// База данных.
type DB struct {
	pool *sql.DB
}

// Публикация, получаемая из RSS.
type Post struct {
	ID      int    // номер записи
	Title   string // заголовок публикации
	Content string // содержание публикации
	PubTime int64  // время публикации
	Link    string // ссылка на источник
}

func New() (*DB, error) {

	connString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		username,
		password,
		hostname,
		port,
		dbName,
	)

	log.Println("connString: ", connString)

	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, fmt.Errorf("mysql err: %s", err)
	}

	return &DB{
		pool: db,
	}, nil
}

func (db *DB) StoreNews(news []Post) error {

	log.Println("news count: ", len(news))

	for _, post := range news {
		query := fmt.Sprintf("INSERT INTO news (`title`, `content`, `pub_time`, `link`) VALUES ('%s', '%s', %d, '%s')",
			post.Title,
			post.Content,
			post.PubTime,
			post.Link)

		_, err := db.pool.Query(query)
		if err != nil {

			return err
		}
	}
	return nil
}

// News возвращает последние новости из БД.
func (db *DB) News(n int) ([]Post, error) {
	if n == 0 {
		n = 10
	}

	query := fmt.Sprintf("SELECT id, title, content, pub_time, link FROM news ORDER BY pub_time LIMIT %d", n)

	rows, err := db.pool.Query(query)

	if err != nil {
		return nil, err
	}

	var news []Post
	for rows.Next() {
		var p Post

		if err != nil {
			return nil, err
		}

		err = rows.Scan(
			&p.ID,
			&p.Title,
			&p.Content,
			&p.PubTime,
			&p.Link,
		)
		if err != nil {
			return nil, err
		}
		news = append(news, p)
	}
	return news, rows.Err()
}
