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

	// Extracts register & resolve endpoint
	if dic, ok := metadata.Configuration.(map[interface{}]interface{}); ok {
		endpoint = fmt.Sprintf("%s", dic["endpointAddress"])
		resolver.registerEndpoint = fmt.Sprintf("%s/register", endpoint)
		resolver.resolveEndpoint = fmt.Sprintf("%s/resolve", endpoint)
	}
	if endpoint == "" {
		return errors.New("service registry endpoint is not configured")
	}

	// Extracts AppID, HostAddress and Port
	props := metadata.Properties
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

	// Register service (application)
	var request = RegisterRequest{appId, hostAddress, port}
	payload, err := json.Marshal(request)
	if err != nil {
		return errors.New("fail to marshal register request")
	}
	_, err = http.Post(resolver.registerEndpoint, "application/json", bytes.NewBuffer(payload))

	if err == nil {
		resolver.logger.Infof("App '%s (%s:%d)' is successfully registered.", request.Id, request.HostAddress, request.Port)
	}
	return err
}

func (resolver *Resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {

	// Invoke resolve service and get resolved target app's endpoint ("{ip}:{port}")
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
	return &Resolver{
		logger: logger,
	}
}
