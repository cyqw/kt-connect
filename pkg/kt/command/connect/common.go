package connect

import (
	"fmt"
	"github.com/alibaba/kt-connect/pkg/common"
	opt "github.com/alibaba/kt-connect/pkg/kt/command/options"
	"github.com/alibaba/kt-connect/pkg/kt/service/cluster"
	"github.com/alibaba/kt-connect/pkg/kt/service/dns"
	"github.com/alibaba/kt-connect/pkg/kt/service/tun"
	"github.com/alibaba/kt-connect/pkg/kt/transmission"
	"github.com/alibaba/kt-connect/pkg/kt/util"
	"github.com/rs/zerolog/log"
	coreV1 "k8s.io/api/core/v1"
	"strings"
	"sync/atomic"
	"time"
)

func setupDns(shadowPodName, shadowPodIp string) error {
	if strings.HasPrefix(opt.Get().Connect.DnsMode, util.DnsModeHosts) {
		log.Info().Msgf("Setting up dns in hosts mode")
		dump2HostsNamespaces := ""
		pos := len(util.DnsModeHosts)
		if len(opt.Get().Connect.DnsMode) > pos+1 && opt.Get().Connect.DnsMode[pos:pos+1] == ":" {
			dump2HostsNamespaces = opt.Get().Connect.DnsMode[pos+1:]
		}
		if err := dumpToHost(dump2HostsNamespaces); err != nil {
			return err
		}
	} else if opt.Get().Connect.DnsMode == util.DnsModePodDns {
		log.Info().Msgf("Setting up dns in pod mode")
		return dns.SetNameServer(shadowPodIp)
	} else if strings.HasPrefix(opt.Get().Connect.DnsMode, util.DnsModeLocalDns) {
		log.Info().Msgf("Setting up dns in local mode")
		svcToIp, headlessPods := getServiceHosts(opt.Get().Global.Namespace, true)
		if err := dns.DumpHosts(svcToIp, ""); err != nil {
			return err
		}
		watchServicesAndPods(opt.Get().Global.Namespace, svcToIp, headlessPods, true)

		forwardedPodPort := util.GetRandomTcpPort()
		if _, err := transmission.SetupPortForwardToLocal(shadowPodName, common.StandardDnsPort, forwardedPodPort); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("invalid dns mode: '%s', supportted mode are %s, %s, %s", opt.Get().Connect.DnsMode,
			util.DnsModeLocalDns, util.DnsModePodDns, util.DnsModeHosts)
	}
	return nil
}

func getDnsOrder(dnsMode string) []string {
	if !strings.Contains(dnsMode, ":") {
		return []string{util.DnsOrderCluster, util.DnsOrderUpstream}
	}
	return strings.Split(strings.SplitN(dnsMode, ":", 2)[1], ",")
}

func watchServicesAndPods(namespace string, svcToIp map[string]string, headlessPods []string, shortDomainOnly bool) {
	var foundChange atomic.Bool
	foundChange.Store(false)
	setupTime := time.Now().Unix()
	go func() {
		for {
			time.Sleep(time.Second * 10)
			if foundChange.CompareAndSwap(true, false) {
				updateHostInfo(namespace, shortDomainOnly)
			}
		}
	}()
	go cluster.Ins().WatchService("", namespace,
		func(svc *coreV1.Service) {
			// ignore add service event during watch setup
			if time.Now().Unix()-setupTime > 3 {
				foundChange.Store(true)
			}
		},
		func(svc *coreV1.Service) {
			foundChange.Store(true)
		}, nil)
	go cluster.Ins().WatchPod("", namespace, nil, func(pod *coreV1.Pod) {
		if util.Contains(headlessPods, pod.Name) {
			// it may take some time for new pod get assign an ip
			time.Sleep(5 * time.Second)
			svcToIp, headlessPods = getServiceHosts(namespace, shortDomainOnly)
			_ = dns.DumpHosts(svcToIp, namespace)
		}
	}, nil)
}

func updateHostInfo(namespace string, shortDomainOnly bool) {
	svcToIp, _ := getServiceHosts(namespace, shortDomainOnly)
	_ = dns.DumpHosts(svcToIp, namespace)
	_ = tun.Ins().RestoreRoute()
	_ = setupTunRoute()
}

func dumpToHost(targetNamespaces string) error {
	namespacesToDump := []string{opt.Get().Global.Namespace}
	if targetNamespaces != "" {
		namespacesToDump = []string{}
		for _, ns := range strings.Split(targetNamespaces, ",") {
			namespacesToDump = append(namespacesToDump, ns)
		}
	}
	hosts := map[string]string{}
	for _, namespace := range namespacesToDump {
		log.Debug().Msgf("Search service in %s namespace ...", namespace)
		svcToIp, headlessPods := getServiceHosts(namespace, false)
		watchServicesAndPods(namespace, svcToIp, headlessPods, false)
		for svc, ip := range svcToIp {
			hosts[svc] = ip
		}
	}
	return dns.DumpHosts(hosts, "")
}

func getServiceHosts(namespace string, shortDomainOnly bool) (map[string]string, []string) {
	hosts := make(map[string]string)
	podNames := make([]string, 0)
	services, err := cluster.Ins().GetAllServiceInNamespace(namespace)
	if err == nil {
		for _, service := range services.Items {
			ip := service.Spec.ClusterIP
			if ip == "" || ip == "None" {
				pods, err2 := cluster.Ins().GetPodsByLabel(service.Spec.Selector, namespace)
				if err2 != nil || len(pods.Items) == 0 {
					continue
				}
				for _, p := range pods.Items {
					ip = p.Status.PodIP
					if ip != "" {
						podNames = append(podNames, p.Name)
						break
					}
				}
				log.Debug().Msgf("Headless service found: %s.%s %s", service.Name, namespace, ip)
			} else {
				log.Debug().Msgf("Service found: %s.%s %s", service.Name, namespace, ip)
			}
			if shortDomainOnly {
				hosts[service.Name] = ip
			} else {
				if namespace == opt.Get().Global.Namespace {
					hosts[service.Name] = ip
				}
				hosts[fmt.Sprintf("%s.%s", service.Name, namespace)] = ip
				hosts[fmt.Sprintf("%s.%s.svc.%s", service.Name, namespace, opt.Get().Connect.ClusterDomain)] = ip
			}
		}
	}
	return hosts, podNames
}

func getOrCreateShadow() (string, string, string, error) {
	shadowPodName := fmt.Sprintf("kt-connect-shadow-%s", strings.ToLower(util.RandomString(5)))
	if opt.Get().Connect.ShareShadow {
		shadowPodName = fmt.Sprintf("kt-connect-shadow-daemon")
	}

	endPointIP, podName, privateKeyPath, err := cluster.Ins().GetOrCreateShadow(shadowPodName, getLabels(),
		make(map[string]string), getEnvs(), "", map[int]string{})
	if err != nil {
		return "", "", "", err
	}

	return endPointIP, podName, privateKeyPath, nil
}

func getEnvs() map[string]string {
	envs := make(map[string]string)
	localDomains := dns.GetLocalDomains()
	if localDomains != "" {
		log.Debug().Msgf("Found local domains: %s", localDomains)
		envs[common.EnvVarLocalDomains] = localDomains
	}
	if strings.HasPrefix(opt.Get().Connect.DnsMode, util.DnsModeLocalDns) {
		envs[common.EnvVarDnsProtocol] = "tcp"
	} else {
		envs[common.EnvVarDnsProtocol] = "udp"
	}
	if opt.Get().Global.Debug {
		envs[common.EnvVarLogLevel] = "debug"
	} else {
		envs[common.EnvVarLogLevel] = "info"
	}
	return envs
}

func getLabels() map[string]string {
	labels := map[string]string{
		util.KtRole: util.RoleConnectShadow,
	}
	if opt.Get().Global.UseShadowDeployment {
		labels[util.KtTarget] = util.RandomString(20)
	}
	return labels
}
