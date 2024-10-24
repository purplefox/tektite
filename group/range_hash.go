package group

import "math/big"

var int128Max = new(big.Int).SetBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})

func CalcMemberForHash(hash []byte, numMembers int) int {
	// we  assign each member a contiguous range out of the possible values of the hash, with member n
	// ranges < member n + 1 ranges, and all values associated with a member.

	hashInt := new(big.Int).SetBytes(hash)

	// Calculate range per member
	rangePerMember := new(big.Int).Div(int128Max, big.NewInt(int64(numMembers)))

	// Choose member based on hash value
	indexBigInt := hashInt.Div(hashInt, rangePerMember)
	memberIndex := int(indexBigInt.Uint64())

	if memberIndex == numMembers {
		// Due to rangePerMember not being exact memberIndex might not be in the range 0... (<numMembers> - 1), but
		// can return <numMembers> for a small number of valid has values right at the top of the range. In this
		// case we assign to <numMembers> - 1.
		// However, it is *very* unlikely we would get a hash result in this range, as the hash is well distributed
		// and there the amount of other values dwarves it. So if this code is actually executed something strange has
		// happened.
		memberIndex = numMembers - 1
	}
	return memberIndex
}