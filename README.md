# CSI plugin for LVM2 [![Issues](https://img.shields.io/badge/Issues-JIRA-ff69b4.svg?style=flat)](https://jira.mesosphere.com/issues/?jql=project%20%3D%20DCOS_OSS%20and%20component%20%3D%20csilvm%20)

This is a container storage interface (CSI) plugin for LVM2. It
exposes a CSI-compliant API to a LVM2 volume group (VG). The volume
group (VG) and the physical volumes (PVs) that consists of are passed
to the plugin at launch time as command-line parameters. CSI volumes
map to LVM2 logical volumes (LVs).

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

* `udevadm`
* `lsblk`
* `pvscan` from the lvm2 utils
* `mkfs`
* the filesystem listed as `-default-fs` (defaults to: `xfs`)

The following shared libraries are dynamically linked against and must
be installed:

* `libdevmapper`
* `liblvm2app` (from LVM2)

## SINGLE_NODE_READER_ONLY

It is not possible to bind mount a device as 'ro' and thereby prevent write access to it.

As such, this plugin does not support the `SINGLE_NODE_READER_ONLY` access mode for a
volume of access type `BLOCK_DEVICE`.

## Tests

The tests can be run using `make sudo-test`. As the target suggests, this
target need the current user to be able to run sudo without a password. The
tests create loopback devices, mount and unmount LVM2 logical volumes, etc, and
therefore must be run as root.
