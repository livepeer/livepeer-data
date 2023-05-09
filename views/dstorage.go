package views

import (
	"encoding/base64"
	"strings"

	"github.com/ipfs/go-cid"
)

func ToDStorageURL(playbackID string) string {
	if isValidArweaveTxID(playbackID) {
		return "ar://" + playbackID
	} else if isValidIPFSCID(playbackID) {
		return "ipfs://" + playbackID
	} else if strings.HasPrefix(playbackID, "ipfs://") || strings.HasPrefix(playbackID, "ar://") {
		return playbackID
	}
	return ""
}

func isValidArweaveTxID(txID string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(txID)
	if err != nil {
		return false
	}
	return len(decoded) == 32
}

func isValidIPFSCID(cidStr string) bool {
	_, err := cid.Decode(cidStr)
	return err == nil
}
