package internal

import (
	"math"
	"sort"

	. "github.com/barcostreams/go-client/types"
)

const startToken Token = math.MinInt64

const maxRingSize = 12288 // 3*math.Pow(2, 12)
const chunkSizeUnit = math.MaxUint64 / maxRingSize

func PrimaryBroker(partitionKey string, brokersLength int) int {
	token := HashToken(partitionKey)
	return GetPrimaryTokenIndex(token, brokersLength)
}

// Gets a token based on a murmur3 hash
func HashToken(key string) Token {
	return Token(Murmur3H1([]byte(key)))
}

// GetPrimaryTokenIndex returns the broker index of the start token in a given range
func GetPrimaryTokenIndex(token Token, tokenRangeLength int) int {
	i := sort.Search(tokenRangeLength, func(i int) bool {
		return GetTokenAtIndex(tokenRangeLength, i) > token
	})

	return i - 1
}

func GetTokenAtIndex(length int, index int) Token {
	// Wrap around
	index = index % length
	return startToken + Token(chunkSizeUnit*getRingFactor(length)*int64(index))
}

func getRingFactor(ringSize int) int64 {
	return int64(maxRingSize / ringSize)
}

func absInt64(num Token) int64 {
	if num < 0 {
		return -int64(num)
	}
	return int64(num)
}
