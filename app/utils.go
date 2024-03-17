package main

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	RESP_ARRAY = '*'
	RESP_SIMPLE = '+'
	RESP_BULK = '$'
	RESP_TERM = "\r\n"
	ECHO = "ECHO"
	PING = "PING"
	SET = "SET"
	GET = "GET"
	PX = "PX"
)

type RESP_Parser struct {
	str string
	position int
}

type RESP_Data struct {
	dataType rune
	value string
}

func (p *RESP_Parser) hasNext() (bool) {
	return p.position < len(p.str)
}

func (p *RESP_Parser) getNext() (RESP_Data, error) {
	var data RESP_Data

	if p.position >= len(p.str) {
		fmt.Println("Error: Position of parser exceeded length of input string")
		return invalidInput()
	}

	c := p.str[p.position]
	p.position++
	switch c {
		case RESP_ARRAY:
			data.dataType = RESP_ARRAY
			end := p.position
			for end+2 < len(p.str) && p.str[end:end+2] != RESP_TERM {
				end++
			}
			data.value = p.str[p.position:end]
			p.position = end+2
		case RESP_BULK:
			data.dataType = RESP_BULK
			end := p.position
			for end+2 < len(p.str) && p.str[end:end+2] != RESP_TERM {
				end++
			}
			length, err := strconv.Atoi(p.str[p.position:end])
			if err != nil {return invalidInput()}
			p.position = end+2

			end = p.position
			for end+2 < len(p.str) && p.str[end:end+2] != RESP_TERM {
				end++
			}
			if length != end-p.position {return invalidInput()}
			data.value = p.str[p.position:end]
			p.position = end+2
		default:
			return invalidInput()
	}
	return data, nil
}

func invalidInput() (RESP_Data, error) {
	err := errors.New("INVALID INPUT")
	return RESP_Data{}, err
}

func Parser(s string) (RESP_Parser) {
	return RESP_Parser{str: s, position: 0}
}