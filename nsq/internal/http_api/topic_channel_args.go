package http_api

import (
	"errors"

	"github.com/nsqio/nsq/internal/protocol"
)

type getter interface {
	Get(key string) (string, error)
}

func GetTopicChannelArgs(rp getter) (string, string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	channelName, err := rp.Get("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !protocol.IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}

//yao
//get topic priority parameters
func GetTopicPriorityArgs(rp getter) (string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", errors.New("MISSING_ARG_TOPIC")
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", errors.New("INVALID_ARG_TOPIC")
	}

	return topicName, nil
}


//yao
//get priority parameters
func GetPriorityArgs(rp getter) (string, error) {
	priorityLevel, err := rp.Get("priority")
	if err != nil {
		return "", errors.New("MISSING_ARG_TOPIC")
	}

	return priorityLevel, nil
}