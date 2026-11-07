package adapters

import "os"

func writeFile(p, c string) error { return os.WriteFile(p, []byte(c), 0o600) }
