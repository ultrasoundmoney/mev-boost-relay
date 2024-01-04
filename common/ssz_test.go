package common

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/attestantio/go-builder-client/api/capella"
	"github.com/attestantio/go-builder-client/spec"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

func TestSSZBuilderSubmission(t *testing.T) {
	// json matches marshalled SSZ
	jsonBytes := LoadGzippedBytes(t, "../testdata/submitBlockPayloadCapella_Goerli.json.gz")

	submitBlockData := new(VersionedSubmitBlockRequest)
	err := json.Unmarshal(jsonBytes, &submitBlockData)
	require.NoError(t, err)

	require.NotNil(t, submitBlockData.Capella)
	marshalledSszBytes, err := submitBlockData.Capella.MarshalSSZ()
	require.NoError(t, err)

	sszBytes := LoadGzippedBytes(t, "../testdata/submitBlockPayloadCapella_Goerli.ssz.gz")
	require.Equal(t, sszBytes, marshalledSszBytes)

	htr, err := submitBlockData.Capella.HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, "0x014c218ba41c2ed5388e7f0ed055e109b83692c772de5c2800140a95a4b66d13", hexutil.Encode(htr[:]))

	// marshalled json matches ssz
	submitBlockSSZ := new(VersionedSubmitBlockRequest)
	err = submitBlockSSZ.UnmarshalSSZ(sszBytes)
	require.NoError(t, err)
	marshalledJSONBytes, err := json.Marshal(submitBlockSSZ)
	require.NoError(t, err)
	// trim white space from expected json
	buffer := new(bytes.Buffer)
	err = json.Compact(buffer, jsonBytes)
	require.NoError(t, err)
	require.Equal(t, buffer.Bytes(), bytes.ToLower(marshalledJSONBytes))
}

func TestSSZGetHeaderResponse(t *testing.T) {
	payload := new(spec.VersionedSignedBuilderBid)

	byteValue, err := os.ReadFile("../testdata/getHeaderResponseCapella_Mainnet.json")
	require.NoError(t, err)

	err = json.Unmarshal(byteValue, &payload)
	require.NoError(t, err)

	ssz, err := payload.Capella.MarshalSSZ()
	require.NoError(t, err)

	sszExpectedBytes, err := os.ReadFile("../testdata/getHeaderResponseCapella_Mainnet.ssz")
	require.NoError(t, err)
	require.Equal(t, sszExpectedBytes, ssz)

	htr, err := payload.Capella.HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, "0x74bfedcdd2da65b4fb14800340ce1abbb202a0dee73aed80b1cf18fb5bc88190", hexutil.Encode(htr[:]))
}

func BenchmarkDecoding(b *testing.B) {
	jsonBytes, err := os.ReadFile("../testdata/getHeaderResponseCapella_Mainnet.json")
	require.NoError(b, err)

	sszBytes, err := os.ReadFile("../testdata/getHeaderResponseCapella_Mainnet.ssz")
	require.NoError(b, err)

	payload := new(spec.VersionedSignedBuilderBid)
	b.Run("json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err = json.Unmarshal(jsonBytes, &payload)
			require.NoError(b, err)
		}
	})
	payload.Capella = new(capella.SignedBuilderBid)
	b.Run("ssz", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err = payload.Capella.UnmarshalSSZ(sszBytes)
			require.NoError(b, err)
		}
	})
}
