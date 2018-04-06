# CSI plugin for LVM2 [![Issues](https://img.shields.io/badge/Issues-JIRA-ff69b4.svg?style=flat)](https://jira.mesosphere.com/issues/?jql=project%20%3D%20DCOS_OSS%20and%20component%20%3D%20csilvm%20and%20status%20not%20in%20(Resolved%2C%20Closed)) [![Build Status](https://jenkins.mesosphere.com/service/jenkins/job/public-csilvm/badge/icon)](https://jenkins.mesosphere.com/service/jenkins/job/public-csilvm/)

This is a [container storage interface (CSI)](https://github.com/container-storage-interface/spec) plugin for LVM2.
It exposes a CSI-compliant API to a LVM2 volume group (VG).
The volume group (VG) and the physical volumes (PVs) that consists of are passed to the plugin at launch time as command-line parameters.
CSI volumes map to LVM2 logical volumes (LVs).

## Destroying a volume group

If the plugin is started with the `-remove-volume-group` command-line
option it will attempt to remove the given volume group when the
`ProbeNode` RPC is called. Most other RPCs will return an error when
running in this mode.

The physical volumes are not destroyed.

## Logical volume naming

The volume group name is specified at startup through the
`-volume-group` argument.

Logical volumes are named according to the following pattern
`<volume-group-name>_<logical-volume-name>`.

## Runtime dependencies

The following command-line utilties must be present in the `PATH`:

* the various lvm2 cli utilities (`pvscan`, `vgcreate`, etc.)
* `udevadm`
* `blkid`
* `mkfs`
* `file`
* the filesystem listed as `-default-fs` (defaults to: `xfs`)

This plugin's tests are run in a centos 7.3.1611 container with
lvm2-2.02.171-8.el7 installed. It should work with newer versions of lvm2 that
are backwards-compatible in their command-line interface. It may work with
older versions.

## Locking

The plugin is completely stateless and performs no locking around operations.
Instead, it relies on LVM2 to lock around operations that are not reentrant.

## SINGLE_NODE_READER_ONLY

It is not possible to bind mount a device as 'ro' and thereby prevent write access to it.

As such, this plugin does not support the `SINGLE_NODE_READER_ONLY` access mode for a
volume of access type `BLOCK_DEVICE`.

## Tests

The tests can be run using `make sudo-test`. As the target suggests, this
target need the current user to be able to run sudo without a password. The
tests create loopback devices, mount and unmount LVM2 logical volumes, etc, and
therefore must be run as root.

# Issues

This project uses JIRA instead of GitHub issues to track bugs and feature requests.
You may review the [currently open issues](https://jira.mesosphere.com/issues/?jql=project%20%3D%20DCOS_OSS%20and%20component%20%3D%20csilvm%20and%20status%20not%20in%20(Resolved%2C%20Closed)).
You may also create a new [bug](https://jira.mesosphere.com/secure/CreateIssueDetails!init.jspa?pid=14105&issuetype=1&components=20732&customfield_12300=3&summary=CSILVM%3a+bug+summary+goes+here&description=Environment%3a%0d%0dWhat+you+attempted+to+do%3a%0d%0dThe+result+you+expected%3a%0d%0dThe+result+you+saw+instead%3a%0d&priority=3) or a new [task](https://jira.mesosphere.com/secure/CreateIssueDetails!init.jspa?pid=14105&issuetype=3&components=20732&customfield_12300=3&summary=CSILVM%3a+task+summary+goes+here&priority=3).
