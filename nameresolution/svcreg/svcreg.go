package svcreg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type Resolver struct {
	logger           logger.Logger
	registerEndpoint string
	resolveEndpoint  string
}

type RegisterRequest struct {
	Id, HostAddress string
	Port            int64
}

func (resolver *Resolver) Init(metadata nameresolution.Metadata) error {

	var endpoint, appId, hostAddress string
	var ok bool

	if dic, ok := metadata.Configuration.(map[interface{}]interface{}); ok {
		endpoint = fmt.Sprintf("%s", dic["endpointAddress"])
	}

	resolver.logger.Infof("Resolver.Endpoint: %s", endpoint)

	props := metadata.Properties

	resolver.registerEndpoint = fmt.Sprintf("%s/register", endpoint)
	resolver.resolveEndpoint = fmt.Sprintf("%s/resolve", endpoint)

	if appId, ok = props[nameresolution.AppID]; !ok {
		return errors.New("AppId does not exist in the name resolution metadata")
	}

	if hostAddress, ok = props[nameresolution.HostAddress]; !ok {
		return errors.New("HostAddress does not exist in the name resolution metadata")
	}

	p, ok := props[nameresolution.DaprPort]
	if !ok {
		return errors.New("DaprPort does not exist in the name resolution metadata")
	}

	port, err := strconv.ParseInt(p, 10, 32)
	if err != nil {
		return errors.New("DaprPort is invalid")
	}

	var request = RegisterRequest{appId, hostAddress, port}
	payload, err := json.Marshal(request)
	if err != nil {
		return errors.New("fail to marshal register request")
	}
	_, err = http.Post(resolver.registerEndpoint, "application/json", bytes.NewBuffer(payload))
	return err
}

func (resolver *Resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	response, err := http.Post(resolver.resolveEndpoint, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	result, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

func NewResolver(logger logger.Logger) *Resolver {

	return &Resolver{logger, "", ""}
}
