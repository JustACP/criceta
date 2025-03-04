package constant

import "fmt"

type Version struct {
	Major int64
	Minor int64
	Patch int64
}

func (v *Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

func (v *Version) EqualTo(dst *Version) bool {
	if dst.Major != v.Major {
		return false
	}

	if dst.Minor != v.Minor {
		return false
	}

	if dst.Patch != v.Patch {
		return false
	}

	return true
}

func (v *Version) LessThan(dst *Version) bool {
	if dst.Major < v.Major {
		return false
	}

	if dst.Minor < v.Minor {
		return false
	}

	if dst.Patch <= v.Patch {
		return false
	}

	return true
}

func (v *Version) MoreThan(dst *Version) bool {
	if dst.Major > v.Major {
		return false
	}

	if dst.Minor > v.Minor {
		return false
	}

	if dst.Patch >= v.Patch {
		return false
	}

	return true
}

var VERSION = Version{
	Major: 0,
	Minor: 0,
	Patch: 1,
}
