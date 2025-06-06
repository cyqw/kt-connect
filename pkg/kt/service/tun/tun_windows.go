package tun

import (
	"fmt"
	opt "github.com/alibaba/kt-connect/pkg/kt/command/options"
	"github.com/alibaba/kt-connect/pkg/kt/util"
	"github.com/rs/zerolog/log"
	wintun "golang.zx2c4.com/wintun"
	"os/exec"
	"strings"
)

type RouteRecord struct {
	TargetRange    string
	InterfaceIndex string
	InterfaceName  string
}

// CheckContext check everything needed for tun setup
func (s *Cli) CheckContext() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to found tun driver: %v", r)
		}
	}()
	if !util.CanRun(exec.Command("netsh")) {
		return fmt.Errorf("failed to found 'netsh' command")
	}
	wintun.RunningVersion()
	return
}

// SetRoute let specified ip range route to tun device
func (s *Cli) SetRoute(ipRange []string, excludeIpRange []string) error {
	var lastErr error
	anyRouteOk := false

	// add by lichp, set ipv6 address
	if opt.Store.Ipv6Cluster == true {
		anyRouteOk, lastErr = s.setIPv6Route(ipRange, excludeIpRange)
	} else {
		for _, r := range ipRange {
			// run command: netsh interface ipv4 add route 172.20.0.0/16 KtConnectTunnel 172.20.0.0
			_, _, err := util.RunAndWait(exec.Command("netsh",
				"interface",
				"ipv4",
				"add",
				"route",
				r,
				s.GetName(),
			))
			if err != nil {
				log.Warn().Msgf("Failed to set route %s to tun device", r)
				lastErr = err
			} else {
				anyRouteOk = true
			}
		}
	}
	if !anyRouteOk {
		return AllRouteFailError{lastErr}
	}
	return lastErr
}

func (s *Cli) setIPv6Route(ipRange []string, excludeIpRange []string) (bool, error) {
	var lastErr error
	anyRouteOk := false
	// add by lichp, set ipv6 address
	var err error
	for i, r := range ipRange {
		if i == 0 {
			// run command: netsh interface ipv6 set address EtConnectTunnel fd11:1111::/32
			_, _, err = util.RunAndWait(exec.Command("netsh",
				"interface",
				"ipv6",
				"set",
				"address",
				s.GetName(),
				r,
			))
		} else {
			// run command: netsh interface ipv6 add address EtConnectTunnel fd11:1112::/32
			_, _, err = util.RunAndWait(exec.Command("netsh",
				"interface",
				"ipv6",
				"add",
				"address",
				s.GetName(),
				r,
			))
		}
		if err != nil {
			log.Warn().Msgf("Failed to add ip addr %s to tun device", r)
			lastErr = err
			continue
		} else {
			anyRouteOk = true
		}
		// run command: netsh interface ipv6 add route fd11:1112::/32 EtConnectTunnel fd11:1112::
		_, _, err = util.RunAndWait(exec.Command("netsh",
			"interface",
			"ipv6",
			"add",
			"route",
			r,
			s.GetName(),
			strings.Split(r, "/")[0],
		))
		if err != nil {
			log.Warn().Msgf("Failed to set route %s to tun device", r)
			lastErr = err
		} else {
			anyRouteOk = true
		}
	}
	return anyRouteOk, lastErr
}

// CheckRoute check whether all route rule setup properly
func (s *Cli) CheckRoute(ipRange []string) []string {
	var failedIpRange []string

	ktIdx, _, err := getInterfaceIndex(s)
	if err != nil || ktIdx == "" {
		log.Warn().Msgf("Failed to found et network interface")
	}

	records, err := getKtRouteRecords(s)
	if err != nil {
		log.Warn().Err(err).Msgf("Route check skipped")
		return []string{}
	}

	for _, ir := range ipRange {
		found := false
		for _, r := range records {
			if ir == r.TargetRange && ktIdx == r.InterfaceIndex {
				found = true
				break
			}
		}
		if !found {
			failedIpRange = append(failedIpRange, ir)
		}
	}

	return failedIpRange
}

// RestoreRoute delete route rules made by kt
func (s *Cli) RestoreRoute() error {
	var lastErr error

	_, otherIdx, err := getInterfaceIndex(s)
	if err != nil {
		return err
	}

	records, err := getKtRouteRecords(s)
	if err != nil {
		return err
	}

	for _, r := range records {
		if util.Contains(otherIdx, r.InterfaceIndex) {
			continue
		}
		// run command: netsh interface ipv4 delete route store=persistent 172.20.0.0/16 29 172.20.0.0
		_, _, err = util.RunAndWait(exec.Command("netsh",
			"interface",
			"ipv4",
			"delete",
			"route",
			"store=persistent",
			r.TargetRange,
			r.InterfaceName,
		))
		if err != nil {
			log.Warn().Msgf("Failed to clean route to %s", r.TargetRange)
			lastErr = err
		} else {
			log.Debug().Msgf("Drop route to %s", r.TargetRange)
		}
	}
	return lastErr
}

func (s *Cli) GetName() string {
	return util.TunNameWin
}

func getInterfaceIndex(s *Cli) (string, []string, error) {
	var ktIdx string
	var otherIdx []string

	// run command: netsh interface ipv4 show interfaces
	out, _, err := util.RunAndWait(exec.Command("netsh",
		"interface",
		"ipv4",
		"show",
		"interfaces",
	))
	if err != nil {
		log.Error().Msgf("Failed to get network interfaces")
		return "", nil, err
	}
	_, _ = util.BackgroundLogger.Write([]byte(">> Get interfaces: " + out + util.Eol))

	reachRecord := false
	for _, line := range strings.Split(out, util.Eol) {
		if strings.HasPrefix(line, "--") && strings.HasSuffix(line, "--") {
			reachRecord = true
			continue
		}
		if !reachRecord {
			continue
		}
		idx := strings.SplitN(strings.TrimLeft(line, " "), " ", 2)[0]
		if strings.HasSuffix(line, s.GetName()) {
			ktIdx = idx
		} else {
			otherIdx = append(otherIdx, idx)
		}
	}
	return ktIdx, otherIdx, nil
}

func getKtRouteRecords(s *Cli) ([]RouteRecord, error) {
	records := []RouteRecord{}

	// run command: netsh interface ipv4 show route store=persistent
	out, _, err := util.RunAndWait(exec.Command("netsh",
		"interface",
		"ipv4",
		"show",
		"route",
		"store=persistent",
	))
	if err != nil {
		log.Warn().Msgf("failed to get route table")
		return nil, err
	}
	_, _ = util.BackgroundLogger.Write([]byte(">> Get route: " + out + util.Eol))

	reachRecord := false
	for _, line := range strings.Split(out, util.Eol) {
		if strings.HasPrefix(line, "--") && strings.HasSuffix(line, "--") {
			reachRecord = true
			continue
		}
		if !reachRecord {
			continue
		}
		parts := strings.Split(line, " ")
		ipRange := ""
		idx := ""
		iface := ""
		index := 0
		for i := 0; i < len(parts); i++ {
			if parts[i] != "" {
				if index == 3 {
					ipRange = parts[i]
				} else if index == 4 {
					idx = parts[i]
				} else if index == 5 {
					iface = parts[i]
				} else if index > 5 {
					iface = fmt.Sprintf("%s %s", iface, parts[i])
				}
				index++
			}
		}
		if idx == "" || ipRange == "" || iface == "" {
			continue
		}
		records = append(records, RouteRecord{
			TargetRange:    ipRange,
			InterfaceIndex: idx,
			InterfaceName:  iface,
		})
	}
	return records, nil
}
