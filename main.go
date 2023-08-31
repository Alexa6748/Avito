package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"fmt"
)

type User struct {
	ID       string
	Segments []Segment
}

type Segment struct {
	Name string
}

type Database struct {
	// Поле для хранения подключения к базе данных
	db *sql.DB
}

func NewDatabase() (*Database, error) {
    // Открытие подключения к базе данных PostgreSQL
    connectionString := "host=<HOSTNAME> port=<PORT> user=<USERNAME> password=<PASSWORD> dbname=<DBNAME> sslmode=disable"
    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        return nil, err
    }

    // Проверка подключения к базе данных PostgreSQL
    err = db.Ping()
    if err != nil {
        db.Close()
        return nil, err
    }

    return &Database{db: db}, nil
}

func (d *Database) Close() error {
    // Закрытие подключения к базе данных PostgreSQL
    return d.db.Close()
}

func (d *Database) createSegment(segment Segment) error {
	// Проверка наличия имени сегмента
	if segment.Name == "" {
		return errors.New("Segment name cannot be empty")
	}

	// Код для создания сегмента в базе данных
	_, err := d.db.Exec("INSERT INTO segments (name) VALUES (?)", segment.Name)
	if err != nil {
		return errors.Wrap(err, "Failed to create segment")
	}

	return nil
}

func (d *Database) updateSegment(segment Segment) error {
	// Проверка наличия имени сегмента
	if segment.Name == "" {
		return errors.New("Segment name cannot be empty")
	}

	// Код для обновления сегмента в базе данных
	_, err := d.db.Exec("UPDATE segments SET name = ?", segment.Name)
	if err != nil {
		return errors.Wrap(err, "Failed to update segment")
	}

	return nil
}

func (d *Database) deleteSegment(segmentID string) error {
    // Проверка наличия идентификатора сегмента
    if segmentID == "" {
        return errors.New("Segment ID cannot be empty")
    }

    // Код для удаления сегмента из базы данных
    _, err := d.db.Exec("DELETE FROM segments WHERE id = ?", segmentID)
    if err != nil {
        return errors.Wrap(err, fmt.Sprintf("Failed to delete segment with ID: %s", segmentID))
    }

    return nil
}

func (d *Database) addUserToSegment(userID string, segmentID string) error {
    // Проверка наличия идентификатора пользователя и сегмента
    if userID == "" {
        return errors.New("User ID cannot be empty")
    }
    if segmentID == "" {
        return errors.New("Segment ID cannot be empty")
    }

    // Код для добавления пользователя в сегмент
    _, err := d.db.Exec("INSERT INTO user_segments (user_id, segment_id) VALUES (?, ?)", userID, segmentID)
    if err != nil {
        return errors.Wrap(err, "Failed to add user to segment")
    }

    return nil
}

func (d *Database) removeUserFromSegment(userID string, segmentID string) error {
    // Проверка наличия идентификатора пользователя и сегмента
    if userID == "" {
        return errors.New("User ID cannot be empty")
    }
    if segmentID == "" {
        return errors.New("Segment ID cannot be empty")
    }

    // Код для удаления пользователя из сегмента
    _, err := d.db.Exec("DELETE FROM user_segments WHERE user_id = ? AND segment_id = ?", userID, segmentID)
    if err != nil {
        return errors.Wrap(err, "Failed to remove user from segment")
    }

    return nil
}

func (d *Database) getUserSegments(userID string) ([]Segment, error) {
    // Проверка наличия идентификатора пользователя
    if userID == "" {
        return nil, errors.New("User ID cannot be empty")
    }

    // Код для получения списка сегментов пользователя из базы данных
    rows, err := d.db.Query("SELECT s.name FROM segments s INNER JOIN user_segments us ON s.id = us.segment_id WHERE us.user_id = ?", userID)
    if err != nil {
        return nil, errors.Wrap(err, "Failed to get user segments")
    }
    defer rows.Close()

    segments := []Segment{}
    for rows.Next() {
        var segmentName string
        if err := rows.Scan(&segmentName); err != nil {
            return nil, errors.Wrap(err, "Failed to scan segment name")
        }
        segment := Segment{
            Name: segmentName,
        }
        segments = append(segments, segment)
    }

    if err := rows.Err(); err != nil {
        return nil, errors.Wrap(err, "Error while iterating over user segments")
    }

    return segments, nil
}