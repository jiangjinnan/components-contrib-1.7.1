/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package storagequeue_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/dapr/components-contrib/bindings"
	binding_asq "github.com/dapr/components-contrib/bindings/azure/storagequeues"
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"

	binding_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"

	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	numOfMessages = 10
)

func TestStorageQueue(t *testing.T) {
	log := logger.NewLogger("dapr-components")
	messagesFor1 := watcher.NewOrdered()
	messagesFor2 := watcher.NewOrdered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgsFor1 := make([]string, numOfMessages/2)
		msgsFor2 := make([]string, numOfMessages/2)
		for i := 0; i < numOfMessages/2; i++ {
			msgsFor1[i] = fmt.Sprintf("standard-binding-1: Message %03d", i)
		}

		for i := numOfMessages / 2; i < numOfMessages; i++ {
			msgsFor2[i-(numOfMessages/2)] = fmt.Sprintf("standard-binding-2: Message %03d", i)
		}

		messagesFor1.ExpectStrings(msgsFor1...)
		messagesFor2.ExpectStrings(msgsFor2...)

		metadata := make(map[string]string)

		ctx.Log("Invoking binding 1!")
		for _, msg := range msgsFor1 {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "standard-binding-1", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		ctx.Log("Invoking binding 2!")
		for _, msg := range msgsFor2 {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "standard-binding-2", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messagesFor1.Assert(ctx, time.Minute)
		messagesFor2.Assert(ctx, time.Minute)

		return nil
	}

	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("standard-binding-1", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messagesFor1.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("standard-binding-2", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messagesFor2.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "servicebusqueue certification").
		// Run the application logic above.
		Step(app.Run("standardApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("standardSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/standard"),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("send and wait", test).
		Run()
}

func TestAzureStorageQueueTTLs(t *testing.T) {
	log := logger.NewLogger("dapr-components")
	ttlMessages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	ttlTest := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		ctx.Logf("Sending messages for expiration.")
		for i := 0; i < numOfMessages; i++ {
			msg := fmt.Sprintf("Expiring message %d", i)

			metadata := make(map[string]string)

			// Send to the queue with TTL.
			queueTTLReq := &daprClient.InvokeBindingRequest{Name: "queue-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, queueTTLReq)
			require.NoError(ctx, err, "error publishing message")

			// Send message with TTL set in yaml file
			messageTTLReq := &daprClient.InvokeBindingRequest{Name: "msg-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			messageTTLReq.Metadata["ttlInSeconds"] = "20"
			err = client.InvokeOutputBinding(ctx, messageTTLReq)
			require.NoError(ctx, err, "error publishing message")

			// Send message with TTL to ensure it overwrites Queue TTL.
			mixedTTLReq := &daprClient.InvokeBindingRequest{Name: "overwrite-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			mixedTTLReq.Metadata["ttlInSeconds"] = "10"
			err = client.InvokeOutputBinding(ctx, mixedTTLReq)
			require.NoError(ctx, err, "error publishing message")
		}

		// Wait for double the TTL after sending the last message.
		time.Sleep(time.Second * 20)
		return nil
	}

	ttlApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("queue-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("msg-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("overwrite-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	freshPorts, _ := dapr_testing.GetFreePorts(2)

	flow.New(t, "storagequeue ttl certification").
		// Run the application logic above.
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("ttlSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/ttl"),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("send ttl messages", ttlTest).
		Step("stop initial sidecar", sidecar.Stop("ttlSidecar")).
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("appSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(freshPorts[0]),
			embedded.WithDaprHTTPPort(freshPorts[1]),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("verify no messages", func(ctx flow.Context) error {
			// Assertion on the data.
			ttlMessages.Assert(t, time.Minute)
			return nil
		}).
		Run()
}

func TestAzureStorageQueueTTLsWithLessSleepTime(t *testing.T) {
	log := logger.NewLogger("dapr-components")
	ttlMessages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	ttlTest := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		ctx.Logf("Sending messages for expiration.")
		for i := 0; i < numOfMessages; i++ {
			msg := fmt.Sprintf("Expiring message %d", i)

			metadata := make(map[string]string)

			// Send message with TTL set in yaml file
			messageTTLReq := &daprClient.InvokeBindingRequest{Name: "msg-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			messageTTLReq.Metadata["ttlInSeconds"] = "20"
			err = client.InvokeOutputBinding(ctx, messageTTLReq)
			require.NoError(ctx, err, "error publishing message")
		}

		// Wait for double the TTL after sending the last message.
		time.Sleep(time.Second * 1)
		return nil
	}

	ttlApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("msg-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ttlMessages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	freshPorts, _ := dapr_testing.GetFreePorts(2)

	flow.New(t, "storagequeue ttl certification").
		// Run the application logic above.
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("ttlSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/ttl"),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("send ttl messages", ttlTest).
		Step("stop initial sidecar", sidecar.Stop("ttlSidecar")).
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("appSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(freshPorts[0]),
			embedded.WithDaprHTTPPort(freshPorts[1]),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("verify no messages", func(ctx flow.Context) error {
			// Assertion on the data.
			ttlMessages.Assert(t, time.Minute)
			return nil
		}).
		Run()
}

func TestAzureStorageQueueForDecode(t *testing.T) {
	log := logger.NewLogger("dapr-components")
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	testDecode := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)
		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)
			dataBytes := []byte(msg)
			dataBytes = []byte(base64.StdEncoding.EncodeToString(dataBytes))
			req := &daprClient.InvokeBindingRequest{Name: "decode-binding", Operation: "create", Data: dataBytes, Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)
		return nil
	}

	decodeApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("decode-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Decoded message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "storagequeue decode certification").
		// Run the application logic above.
		Step(app.Run("standardApp", fmt.Sprintf(":%d", appPort), decodeApplication)).
		Step(sidecar.Run("standardSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/decode"),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("send and wait", testDecode).
		Run()
}

func TestAzureStorageQueueRetriesOnError(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	testRetry := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)
		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		// Send events that the application above will observe.
		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "retry-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)

		return nil
	}
	// Application logic that tracks messages from a topic.
	retryApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 10)

		// Setup the input binding endpoint.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("retry-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				if err := sim(); err != nil {
					ctx.Logf("Failing message: %s", string(in.Data))
					return nil, err
				}
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	flow.New(t, "servicebusqueue retry certification").
		// Run the application logic above.
		Step(app.Run("retryApp", fmt.Sprintf(":%d", appPort), retryApplication)).
		Step(sidecar.Run("retrySidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/retry"),
			runtime.WithOutputBindings(
				binding_loader.NewOutput("azure.storagequeues", func() bindings.OutputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithInputBindings(
				binding_loader.NewInput("azure.storagequeues", func() bindings.InputBinding {
					return binding_asq.NewAzureStorageQueues(log)
				}),
			),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("interrupt network", network.InterruptNetwork(time.Minute, []string{}, []string{}, "443")).
		Step("send and wait", testRetry).
		Run()
}
