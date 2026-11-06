package machines

func CanTransitionDualPublish(current string, target string) bool {
	switch current {
	case "ACTIVE":
		return target == "DRAINING" || target == "EXTENDED" || target == "DECOMMISSIONED"
	case "EXTENDED":
		return target == "DRAINING" || target == "DECOMMISSIONED"
	case "DRAINING":
		return target == "DECOMMISSIONED" || target == "EXTENDED"
	case "DECOMMISSIONED":
		return false
	default:
		return false
	}
}

