package dbop

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type DSN struct {
	Host string            `json:"host" yaml:"host"`
	Port int               `json:"port" yaml:"port"`
	User string            `json:"user" yaml:"user"`
	Pass string            `json:"pass" yaml:"pass"`
	Name string            `json:"name" yaml:"name"`
	Args map[string]string `json:"args" yaml:"args"`
}

func (n DSN) String(driver string) (string, error) {
	if n.Host == "" {
		n.Host = "127.0.0.1"
	}
	if n.User == "" && n.Host != "127.0.0.1" && n.Host != "localhost" && n.Host != "::1" {
		return "", errors.New("invalid DSN: missing 'user'")
	}
	if n.Args == nil {
		n.Args = make(map[string]string)
	}
	switch driver {
	case "mysql":
		if _, ok := n.Args["loc"]; !ok {
			n.Args["loc"] = "Local"
		}
		if _, ok := n.Args["parseTime"]; !ok {
			n.Args["parseTime"] = "true"
		}
		if n.Port == 0 {
			n.Port = 3306
		}
		ident := n.User
		if ident == "" {
			ident = "root"
		}
		if n.Pass != "" {
			ident += ":" + n.Pass
		}
		addr := net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
		dsn := fmt.Sprintf("%s@tcp(%s)/%s", ident, addr, n.Name)
		var args []string
		for k, v := range n.Args {
			args = append(args, fmt.Sprintf("%s=%s", k, url.PathEscape(v)))
		}
		return dsn + "?" + strings.Join(args, "&"), nil
	case "postgres":
		if _, ok := n.Args["sslmode"]; !ok {
			n.Args["sslmode"] = "disable"
		}
		if n.Port == 0 {
			n.Port = 5432
		}
		if n.User == "" {
			n.User = "postgres"
		}
		args := []string{fmt.Sprintf("host=%s", n.Host)}
		args = append(args, fmt.Sprintf("port=%d", n.Port))
		args = append(args, fmt.Sprintf("user=%s", n.User))
		if n.Pass != "" {
			args = append(args, fmt.Sprintf("password=%s", n.Pass))
		}
		args = append(args, fmt.Sprintf("dbname=%s", n.Name))
		for k, v := range n.Args {
			args = append(args, fmt.Sprintf("%s=%s", k, v))
		}
		return strings.Join(args, " "), nil
	default:
		return "", fmt.Errorf("invalid DSN: unkown driver %q", driver)
	}
}

func (n DSN) Connect(driver string) (*sql.DB, error) {
	dsn, err := n.String(driver)
	if err != nil {
		return nil, err
	}
	return sql.Open(driver, dsn)
}
