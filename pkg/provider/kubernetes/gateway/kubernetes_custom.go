package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/job"
	"github.com/traefik/traefik/v3/pkg/observability/logs"
	"github.com/traefik/traefik/v3/pkg/safe"
	"github.com/traefik/traefik/v3/pkg/tls"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	gatev1 "sigs.k8s.io/gateway-api/apis/v1"
)

type GatewayConfiguration struct {
	Provider      string
	Configuration dynamic.Configuration
	Gateway       gatev1.Gateway
}

func (p *Provider) ProvideGatewayConfigurations(configurationChan chan<- GatewayConfiguration, pool *safe.Pool) error {

	logger := log.With().Str(logs.ProviderName, providerName).Logger()
	ctxLog := logger.WithContext(context.Background())

	pool.GoCtx(func(ctxPool context.Context) {
		operation := func() error {
			eventsChan, err := p.client.WatchAll(p.Namespaces, ctxPool.Done())
			if err != nil {
				logger.Error().Err(err).Msg("Error watching kubernetes events")
				timer := time.NewTimer(1 * time.Second)
				select {
				case <-timer.C:
					return err
				case <-ctxPool.Done():
					return nil
				}
			}

			throttleDuration := time.Duration(p.ThrottleDuration)
			throttledChan := throttleEvents(ctxLog, throttleDuration, pool, eventsChan)
			if throttledChan != nil {
				eventsChan = throttledChan
			}

			for {
				select {
				case <-ctxPool.Done():
					return nil
				case _ = <-eventsChan:
					// Note that event is the *first* event that came in during this throttling interval -- if we're hitting our throttle, we may have dropped events.
					// This is fine, because we don't treat different event types differently.
					// But if we do in the future, we'll need to track more information about the dropped events.
					confs := p.loadGatewayConfigurations(ctxLog)

					for i := range confs {
						configurationChan <- *confs[i]
					}

					// confHash, err := hashstructure.Hash(conf, nil)
					// switch {
					// case err != nil:
					// 	logger.Error().Msg("Unable to hash the configuration")
					// case p.lastConfiguration.Get() == confHash:
					// 	logger.Debug().Msgf("Skipping Kubernetes event kind %T", event)
					// default:
					// 	p.lastConfiguration.Set(confHash)
					// 	configurationChan <- dynamic.Message{
					// 		ProviderName:  providerName,
					// 		Configuration: conf,
					// 	}
					// }

					// If we're throttling,
					// we sleep here for the throttle duration to enforce that we don't refresh faster than our throttle.
					// time.Sleep returns immediately if p.ThrottleDuration is 0 (no throttle).
					time.Sleep(throttleDuration)
				}
			}
		}

		notify := func(err error, time time.Duration) {
			logger.Error().Err(err).Msgf("Provider error, retrying in %s", time)
		}
		err := backoff.RetryNotify(safe.OperationWithRecover(operation), backoff.WithContext(job.NewBackOff(backoff.NewExponentialBackOff()), ctxPool), notify)
		if err != nil {
			logger.Error().Err(err).Msg("Cannot retrieve data")
		}
	})

	return nil
}

func (p *Provider) loadGatewayConfigurations(ctx context.Context) []*GatewayConfiguration {
	addresses, err := p.gatewayAddresses()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Unable to get Gateway status addresses")
		return nil
	}

	gatewayClasses, err := p.client.ListGatewayClasses()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Unable to list GatewayClasses")
		return nil
	}

	var supportedFeatures []gatev1.SupportedFeature
	for _, feature := range SupportedFeatures() {
		supportedFeatures = append(supportedFeatures, gatev1.SupportedFeature{Name: gatev1.FeatureName(feature)})
	}
	slices.SortFunc(supportedFeatures, func(a, b gatev1.SupportedFeature) int {
		return strings.Compare(string(a.Name), string(b.Name))
	})

	gatewayClassNames := map[string]struct{}{}
	for _, gatewayClass := range gatewayClasses {
		if gatewayClass.Spec.ControllerName != controllerName {
			continue
		}

		gatewayClassNames[gatewayClass.Name] = struct{}{}

		status := gatev1.GatewayClassStatus{
			Conditions: upsertGatewayClassConditionAccepted(gatewayClass.Status.Conditions, metav1.Condition{
				Type:               string(gatev1.GatewayClassConditionStatusAccepted),
				Status:             metav1.ConditionTrue,
				ObservedGeneration: gatewayClass.Generation,
				Reason:             "Handled",
				Message:            "Handled by Traefik controller",
				LastTransitionTime: metav1.Now(),
			}),
			SupportedFeatures: supportedFeatures,
		}

		if err := p.client.UpdateGatewayClassStatus(ctx, gatewayClass.Name, status); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("gateway_class", gatewayClass.Name).
				Msg("Unable to update GatewayClass status")
		}
	}

	var configurations []*GatewayConfiguration
	for _, gateway := range p.client.ListGateways() {
		if _, ok := gatewayClassNames[string(gateway.Spec.GatewayClassName)]; !ok {
			continue
		}
		conf := p.loadGatewayConfiguration(ctx, gateway, addresses)
		configurations = append(configurations, &GatewayConfiguration{
			Provider:      providerName,
			Configuration: *conf,
			Gateway:       *gateway,
		})
	}

	return configurations
}

func (p *Provider) loadGatewayConfiguration(ctx context.Context, gateway *gatev1.Gateway, addresses []gatev1.GatewayStatusAddress) *dynamic.Configuration {
	conf := &dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{
			Routers:           map[string]*dynamic.Router{},
			Middlewares:       map[string]*dynamic.Middleware{},
			Services:          map[string]*dynamic.Service{},
			ServersTransports: map[string]*dynamic.ServersTransport{},
		},
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
		TLS: &dynamic.TLSConfiguration{},
	}

	logger := log.Ctx(ctx).With().
		Str("gateway", gateway.Name).
		Str("namespace", gateway.Namespace).
		Logger()

	var gatewayListeners = p.loadGatewayListenersCustom(logger.WithContext(ctx), gateway, conf)

	p.loadHTTPRoutes(ctx, gatewayListeners, conf)

	p.loadGRPCRoutes(ctx, gatewayListeners, conf)

	if p.ExperimentalChannel {
		p.loadTCPRoutes(ctx, gatewayListeners, conf)
		p.loadTLSRoutes(ctx, gatewayListeners, conf)
	}

	var listeners []gatewayListener
	for _, listener := range gatewayListeners {
		if listener.GWName == gateway.Name && listener.GWNamespace == gateway.Namespace {
			listeners = append(listeners, listener)
		}
	}

	gatewayStatus, errConditions := p.makeGatewayStatus(gateway, listeners, addresses)
	if len(errConditions) > 0 {
		messages := map[string]struct{}{}
		for _, condition := range errConditions {
			messages[condition.Message] = struct{}{}
		}
		var conditionsErr error
		for message := range messages {
			conditionsErr = multierror.Append(conditionsErr, errors.New(message))
		}
		logger.Error().
			Err(conditionsErr).
			Msg("Gateway Not Accepted")
	}

	if err := p.client.UpdateGatewayStatus(ctx, ktypes.NamespacedName{Name: gateway.Name, Namespace: gateway.Namespace}, gatewayStatus); err != nil {
		logger.Warn().
			Err(err).
			Msg("Unable to update Gateway status")
	}

	return conf
}

func (p *Provider) loadGatewayListenersCustom(ctx context.Context, gateway *gatev1.Gateway, conf *dynamic.Configuration) []gatewayListener {
	tlsConfigs := make(map[string]*tls.CertAndStores)
	allocatedListeners := make(map[string]struct{})
	gatewayListeners := make([]gatewayListener, len(gateway.Spec.Listeners))

	for i, listener := range gateway.Spec.Listeners {
		gatewayListeners[i] = gatewayListener{
			Name:         string(listener.Name),
			GWName:       gateway.Name,
			GWNamespace:  gateway.Namespace,
			GWGeneration: gateway.Generation,
			Port:         listener.Port,
			Protocol:     listener.Protocol,
			TLS:          listener.TLS,
			Hostname:     listener.Hostname,
			Status: &gatev1.ListenerStatus{
				Name:           listener.Name,
				SupportedKinds: []gatev1.RouteGroupKind{},
				Conditions:     []metav1.Condition{},
			},
		}

		// ep, err := p.entryPointName(listener.Port, listener.Protocol)
		// if err != nil {
		// 	// update "Detached" status with "PortUnavailable" reason
		// 	gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
		// 		Type:               string(gatev1.ListenerConditionAccepted),
		// 		Status:             metav1.ConditionFalse,
		// 		ObservedGeneration: gateway.Generation,
		// 		LastTransitionTime: metav1.Now(),
		// 		Reason:             string(gatev1.ListenerReasonPortUnavailable),
		// 		Message:            fmt.Sprintf("Cannot find entryPoint for Gateway: %v", err),
		// 	})

		// 	continue
		// }
		gatewayListeners[i].EPName = string(listener.Name)

		var err error

		allowedRoutes := ptr.Deref(listener.AllowedRoutes, gatev1.AllowedRoutes{Namespaces: &gatev1.RouteNamespaces{From: ptr.To(gatev1.NamespacesFromSame)}})
		gatewayListeners[i].AllowedNamespaces, err = p.allowedNamespaces(gateway.Namespace, allowedRoutes.Namespaces)
		if err != nil {
			// update "ResolvedRefs" status true with "InvalidRoutesRef" reason
			gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
				Type:               string(gatev1.ListenerConditionResolvedRefs),
				Status:             metav1.ConditionFalse,
				ObservedGeneration: gateway.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "InvalidRouteNamespacesSelector", // Should never happen as the selector is validated by kubernetes
				Message:            fmt.Sprintf("Invalid route namespaces selector: %v", err),
			})

			continue
		}

		supportedKinds, conditions := supportedRouteKinds(listener.Protocol, p.ExperimentalChannel)
		if len(conditions) > 0 {
			gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, conditions...)
			continue
		}

		routeKinds, conditions := allowedRouteKinds(gateway, listener, supportedKinds)
		for _, kind := range routeKinds {
			gatewayListeners[i].AllowedRouteKinds = append(gatewayListeners[i].AllowedRouteKinds, string(kind.Kind))
		}
		gatewayListeners[i].Status.SupportedKinds = routeKinds
		if len(conditions) > 0 {
			gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, conditions...)
			continue
		}

		listenerKey := makeListenerKey(listener)

		if _, ok := allocatedListeners[listenerKey]; ok {
			gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
				Type:               string(gatev1.ListenerConditionConflicted),
				Status:             metav1.ConditionTrue,
				ObservedGeneration: gateway.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "DuplicateListener",
				Message:            "A listener with same protocol, port and hostname already exists",
			})

			continue
		}

		allocatedListeners[listenerKey] = struct{}{}

		if (listener.Protocol == gatev1.HTTPProtocolType || listener.Protocol == gatev1.TCPProtocolType) && listener.TLS != nil {
			gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
				Type:               string(gatev1.ListenerConditionAccepted),
				Status:             metav1.ConditionFalse,
				ObservedGeneration: gateway.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "InvalidTLSConfiguration", // TODO check the spec if a proper reason is introduced at some point
				Message:            "TLS configuration must no be defined when using HTTP or TCP protocol",
			})

			continue
		}

		// TLS
		if listener.Protocol == gatev1.HTTPSProtocolType || listener.Protocol == gatev1.TLSProtocolType {
			if listener.TLS == nil || (len(listener.TLS.CertificateRefs) == 0 && listener.TLS.Mode != nil && *listener.TLS.Mode != gatev1.TLSModePassthrough) {
				// update "Detached" status with "UnsupportedProtocol" reason
				gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
					Type:               string(gatev1.ListenerConditionAccepted),
					Status:             metav1.ConditionFalse,
					ObservedGeneration: gateway.Generation,
					LastTransitionTime: metav1.Now(),
					Reason:             "InvalidTLSConfiguration", // TODO check the spec if a proper reason is introduced at some point
					Message: fmt.Sprintf("No TLS configuration for Gateway Listener %s:%d and protocol %q",
						listener.Name, listener.Port, listener.Protocol),
				})

				continue
			}

			var tlsModeType gatev1.TLSModeType
			if listener.TLS.Mode != nil {
				tlsModeType = *listener.TLS.Mode
			}

			isTLSPassthrough := tlsModeType == gatev1.TLSModePassthrough

			if isTLSPassthrough && len(listener.TLS.CertificateRefs) > 0 {
				// https://gateway-api.sigs.k8s.io/v1alpha2/references/spec/#gateway.networking.k8s.io/v1alpha2.GatewayTLSConfig
				log.Ctx(ctx).Warn().Msg("In case of Passthrough TLS mode, no TLS settings take effect as the TLS session from the client is NOT terminated at the Gateway")
			}

			// Allowed configurations:
			// Protocol TLS -> Passthrough -> TLSRoute/TCPRoute
			// Protocol TLS -> Terminate -> TLSRoute/TCPRoute
			// Protocol HTTPS -> Terminate -> HTTPRoute
			if listener.Protocol == gatev1.HTTPSProtocolType && isTLSPassthrough {
				gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
					Type:               string(gatev1.ListenerConditionAccepted),
					Status:             metav1.ConditionFalse,
					ObservedGeneration: gateway.Generation,
					LastTransitionTime: metav1.Now(),
					Reason:             string(gatev1.ListenerReasonUnsupportedProtocol),
					Message:            "HTTPS protocol is not supported with TLS mode Passthrough",
				})

				continue
			}

			if !isTLSPassthrough {
				if len(listener.TLS.CertificateRefs) == 0 {
					// update "ResolvedRefs" status true with "InvalidCertificateRef" reason
					gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
						Type:               string(gatev1.ListenerConditionResolvedRefs),
						Status:             metav1.ConditionFalse,
						ObservedGeneration: gateway.Generation,
						LastTransitionTime: metav1.Now(),
						Reason:             string(gatev1.ListenerReasonInvalidCertificateRef),
						Message:            "One TLS CertificateRef is required in Terminate mode",
					})

					continue
				}

				// TODO Should we support multiple certificates?
				certificateRef := listener.TLS.CertificateRefs[0]

				if certificateRef.Kind == nil || *certificateRef.Kind != "Secret" ||
					certificateRef.Group == nil || (*certificateRef.Group != "" && *certificateRef.Group != groupCore) {
					// update "ResolvedRefs" status true with "InvalidCertificateRef" reason
					gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
						Type:               string(gatev1.ListenerConditionResolvedRefs),
						Status:             metav1.ConditionFalse,
						ObservedGeneration: gateway.Generation,
						LastTransitionTime: metav1.Now(),
						Reason:             string(gatev1.ListenerReasonInvalidCertificateRef),
						Message:            fmt.Sprintf("Unsupported TLS CertificateRef group/kind: %s/%s", groupToString(certificateRef.Group), kindToString(certificateRef.Kind)),
					})

					continue
				}

				certificateNamespace := gateway.Namespace
				if certificateRef.Namespace != nil && string(*certificateRef.Namespace) != gateway.Namespace {
					certificateNamespace = string(*certificateRef.Namespace)
				}

				if err := p.isReferenceGranted(kindGateway, gateway.Namespace, groupCore, "Secret", string(certificateRef.Name), certificateNamespace); err != nil {
					gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions, metav1.Condition{
						Type:               string(gatev1.ListenerConditionResolvedRefs),
						Status:             metav1.ConditionFalse,
						ObservedGeneration: gateway.Generation,
						LastTransitionTime: metav1.Now(),
						Reason:             string(gatev1.ListenerReasonRefNotPermitted),
						Message:            fmt.Sprintf("Cannot load CertificateRef %s/%s: %s", certificateNamespace, certificateRef.Name, err),
					})

					continue
				}

				configKey := certificateNamespace + "/" + string(certificateRef.Name)
				if _, tlsExists := tlsConfigs[configKey]; !tlsExists {
					tlsConf, err := p.getTLS(certificateRef.Name, certificateNamespace)
					if err != nil {
						// update "ResolvedRefs" status false with "InvalidCertificateRef" reason
						// update "Programmed" status false with "Invalid" reason
						gatewayListeners[i].Status.Conditions = append(gatewayListeners[i].Status.Conditions,
							metav1.Condition{
								Type:               string(gatev1.ListenerConditionResolvedRefs),
								Status:             metav1.ConditionFalse,
								ObservedGeneration: gateway.Generation,
								LastTransitionTime: metav1.Now(),
								Reason:             string(gatev1.ListenerReasonInvalidCertificateRef),
								Message:            fmt.Sprintf("Error while retrieving certificate: %v", err),
							},
							metav1.Condition{
								Type:               string(gatev1.ListenerConditionProgrammed),
								Status:             metav1.ConditionFalse,
								ObservedGeneration: gateway.Generation,
								LastTransitionTime: metav1.Now(),
								Reason:             string(gatev1.ListenerReasonInvalid),
								Message:            fmt.Sprintf("Error while retrieving certificate: %v", err),
							},
						)

						continue
					}
					tlsConfigs[configKey] = tlsConf
				}
			}
		}

		gatewayListeners[i].Attached = true
	}

	if len(tlsConfigs) > 0 {
		conf.TLS.Certificates = append(conf.TLS.Certificates, getTLSConfig(tlsConfigs)...)
	}

	return gatewayListeners
}
