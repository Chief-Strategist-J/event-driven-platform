package ports

type HasherPort interface {
	HashBytes(data []byte) int32
	CalculatePartition(keyBytes []byte, partitionCount int) (partition int, rawHash int32, maskedHash int32, err error)
}
