# CSI plugin for LVM2

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

* `lsblk`
* `pvscan` from the lvm2 utils
* `mkfs`
* the filesystem listed as `-default-fs` (defaults to: `xfs`)

The following shared libraries are dynamically linked against and must
be installed:

* `libdevmapper`
* `liblvm2app` (from LVM2)
