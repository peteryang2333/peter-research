#!/usr/bin/env python3
"""Open a TCP ingress port on the OCI security list guarding a given instance.

Finds the instance by its public IP across every subscribed region, walks
instance -> VNIC -> subnet -> security list, and adds an ingress rule only if
an equivalent rule is not already present (idempotent).

    python oci_open_port.py --ip 152.70.194.225 --port 8443
"""
import argparse
import sys

import oci


def find_instance(cfg, target_ip):
    """Return (region, compartment_id, instance, vnic) for the public IP."""
    identity = oci.identity.IdentityClient(cfg)
    tenancy = cfg["tenancy"]

    regions = [r.region_name for r in
               identity.list_region_subscriptions(tenancy).data]
    # Home/default region first - almost always the right one.
    if cfg.get("region") in regions:
        regions.remove(cfg["region"])
        regions.insert(0, cfg["region"])

    comps = [tenancy] + [c.id for c in oci.pagination.list_call_get_all_results(
        identity.list_compartments, tenancy,
        compartment_id_in_subtree=True, access_level="ACCESSIBLE").data
        if c.lifecycle_state == "ACTIVE"]

    for region in regions:
        rcfg = dict(cfg, region=region)
        compute = oci.core.ComputeClient(rcfg)
        net = oci.core.VirtualNetworkClient(rcfg)
        for comp in comps:
            try:
                insts = oci.pagination.list_call_get_all_results(
                    compute.list_instances, comp).data
            except oci.exceptions.ServiceError:
                continue
            for inst in insts:
                if inst.lifecycle_state not in ("RUNNING", "STOPPED"):
                    continue
                try:
                    attachments = compute.list_vnic_attachments(
                        comp, instance_id=inst.id).data
                except oci.exceptions.ServiceError:
                    continue
                for att in attachments:
                    if not att.vnic_id:
                        continue
                    try:
                        vnic = net.get_vnic(att.vnic_id).data
                    except oci.exceptions.ServiceError:
                        continue
                    if vnic.public_ip == target_ip:
                        return region, comp, inst, vnic
    return None, None, None, None


def rule_exists(rules, port):
    for r in rules:
        if r.protocol != "6":                      # 6 = TCP
            continue
        if (r.source or "") not in ("0.0.0.0/0",):
            continue
        opt = r.tcp_options
        if opt is None:                            # all TCP ports allowed
            return True
        dr = opt.destination_port_range
        if dr and dr.min <= port <= dr.max:
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ip", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = oci.config.from_file()
    oci.config.validate_config(cfg)

    region, comp, inst, vnic = find_instance(cfg, args.ip)
    if not inst:
        print(f"ERROR: no instance found with public IP {args.ip}")
        return 1
    print(f"instance : {inst.display_name}  ({region})")
    print(f"vnic     : {vnic.private_ip} / {vnic.public_ip}  subnet={vnic.subnet_id[-16:]}")

    rcfg = dict(cfg, region=region)
    net = oci.core.VirtualNetworkClient(rcfg)
    subnet = net.get_subnet(vnic.subnet_id).data

    # Network security groups take precedence when attached to the VNIC.
    if vnic.nsg_ids:
        print(f"NSGs attached: {len(vnic.nsg_ids)} -> adding rule to first NSG")
        nsg_id = vnic.nsg_ids[0]
        existing = net.list_network_security_group_security_rules(
            nsg_id, direction="INGRESS").data
        for r in existing:
            if (r.protocol == "6" and r.source == "0.0.0.0/0" and
                    (r.tcp_options is None or
                     (r.tcp_options.destination_port_range and
                      r.tcp_options.destination_port_range.min <= args.port
                      <= r.tcp_options.destination_port_range.max))):
                print(f"OK: NSG already allows TCP {args.port}")
                return 0
        if args.dry_run:
            print(f"DRY-RUN: would add TCP {args.port} to NSG {nsg_id[-16:]}")
            return 0
        net.add_network_security_group_security_rules(
            nsg_id,
            oci.core.models.AddNetworkSecurityGroupSecurityRulesDetails(
                security_rules=[
                    oci.core.models.AddSecurityRuleDetails(
                        direction="INGRESS", protocol="6", source="0.0.0.0/0",
                        source_type="CIDR_BLOCK",
                        description=f"peter-research dashboard {args.port}",
                        tcp_options=oci.core.models.TcpOptions(
                            destination_port_range=oci.core.models.PortRange(
                                min=args.port, max=args.port)))]))
        print(f"ADDED: TCP {args.port} ingress on NSG {nsg_id[-16:]}")
        return 0

    sl_id = subnet.security_list_ids[0]
    sl = net.get_security_list(sl_id).data
    print(f"security list: {sl.display_name}  rules={len(sl.ingress_security_rules)}")

    if rule_exists(sl.ingress_security_rules, args.port):
        print(f"OK: security list already allows TCP {args.port}")
        return 0
    if args.dry_run:
        print(f"DRY-RUN: would add TCP {args.port} to {sl.display_name}")
        return 0

    new_rule = oci.core.models.IngressSecurityRule(
        protocol="6", source="0.0.0.0/0", source_type="CIDR_BLOCK",
        is_stateless=False,
        description=f"peter-research dashboard {args.port}",
        tcp_options=oci.core.models.TcpOptions(
            destination_port_range=oci.core.models.PortRange(
                min=args.port, max=args.port)))
    net.update_security_list(
        sl_id, oci.core.models.UpdateSecurityListDetails(
            ingress_security_rules=list(sl.ingress_security_rules) + [new_rule],
            egress_security_rules=sl.egress_security_rules))
    print(f"ADDED: TCP {args.port} ingress on {sl.display_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
