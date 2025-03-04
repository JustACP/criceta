package common

type Enum interface {
	String() string
	FromString(s string) (Enum, error)
}
