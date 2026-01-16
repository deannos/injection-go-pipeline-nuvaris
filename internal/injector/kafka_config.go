package injector

import (
	"fmt"
	"strings"

	"github.com/IBM/sarama"
)

func parseRequiredAcks(v string) (sarama.RequiredAcks, error) {
	switch strings.ToLower(v) {
	case "none", "no_response":
		return sarama.NoResponse, nil
	case "leader", "local", "wait_for_local", "1":
		return sarama.WaitForLocal, nil
	case "all", "wait_for_all", "-1":
		return sarama.WaitForAll, nil
	default:
		return sarama.WaitForAll, fmt.Errorf("invalid kafka requiredAcks: %s", v)
	}
}
