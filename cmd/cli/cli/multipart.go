// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

// multipartUpload manages the multipart upload process
type multipartUpload struct {
	// File info
	filePath  string
	fileSize  int64
	chunkSize int64

	// Upload info
	uploadID string
	bck      cmn.Bck
	objName  string

	// Concurrency control
	wg       cos.WG
	errCh    chan error
	partNums []int
}

// init initializes the multipart upload and creates necessary resources
func (m *multipartUpload) init(c *cli.Context) error {
	if m.chunkSize <= 0 {
		return fmt.Errorf("chunk size must be positive, got %d", m.chunkSize)
	}

	numWorkers, err := parseNumWorkersFlag(c, numPutWorkersFlag)
	if err != nil {
		return err
	}
	uploadID, err := api.CreateMultipartUpload(apiBP, m.bck, m.objName)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}
	m.uploadID = uploadID

	numChunks := (m.fileSize + m.chunkSize - 1) / m.chunkSize
	m.wg = cos.NewLimitedWaitGroup(numWorkers, 0)
	m.errCh = make(chan error, numChunks)
	m.partNums = make([]int, numChunks)

	return nil
}

// do performs the actual multipart upload
func (m *multipartUpload) do(c *cli.Context) error {
	numChunks := (m.fileSize + m.chunkSize - 1) / m.chunkSize

	// Upload each chunk in parallel
	for i := range numChunks {
		offset := i * m.chunkSize
		currentChunkSize := m.chunkSize
		if offset+m.chunkSize > m.fileSize {
			currentChunkSize = m.fileSize - offset
		}
		partNum := int(i + 1) // parts are 1-based

		m.wg.Add(1)
		go m.uploadChunk(c, partNum, offset, currentChunkSize, &m.partNums[i])
	}

	m.wg.Wait()
	close(m.errCh)

	for err := range m.errCh {
		if err != nil {
			return fmt.Errorf("chunk upload failed: %w", err)
		}
	}

	return api.CompleteMultipartUpload(apiBP, m.bck, m.objName, m.uploadID, m.partNums)
}

// uploadChunk uploads a single chunk of the file
func (m *multipartUpload) uploadChunk(c *cli.Context, partNum int, offset, size int64, partNumPtr *int) {
	defer m.wg.Done()

	// Create section reader for this chunk
	reader, err := cos.NewFileSectionHandle(m.filePath, offset, size)
	if err != nil {
		m.errCh <- fmt.Errorf("failed to create section reader for part %d: %w", partNum, err)
		return
	}
	defer reader.Close()

	// Prepare upload part arguments
	putPartArgs := &api.PutPartArgs{
		PutArgs: api.PutArgs{
			BaseParams: apiBP,
			Bck:        m.bck,
			ObjName:    m.objName,
			Reader:     reader,
			Size:       uint64(size),
		},
		PartNumber: partNum,
		UploadID:   m.uploadID,
	}

	// Upload the part
	if err := api.UploadPart(putPartArgs); err != nil {
		m.errCh <- fmt.Errorf("failed to upload part %d: %w", partNum, err)
		return
	}

	// Record successful part number
	*partNumPtr = partNum

	if flagIsSet(c, verboseFlag) {
		fmt.Printf("  Uploaded part %d: offset=%s, size=%s\n",
			partNum, cos.ToSizeIEC(offset, 0), cos.ToSizeIEC(size, 0))
	}
}

// uploadFileInChunks uploads a file in chunks using multipart upload
func uploadFileInChunks(c *cli.Context, filePath string, fileSize, chunkSize int64, bck cmn.Bck, objName string) error {
	upload := &multipartUpload{
		filePath:  filePath,
		fileSize:  fileSize,
		chunkSize: chunkSize,
		bck:       bck,
		objName:   objName,
	}

	if err := upload.init(c); err != nil {
		return err
	}

	if err := upload.do(c); err != nil {
		// Abort multipart upload on error
		if abortErr := api.AbortMultipartUpload(apiBP, upload.bck, upload.objName, upload.uploadID); abortErr != nil {
			fmt.Fprintf(c.App.ErrWriter, "Warning: failed to abort multipart upload: %v\n", abortErr)
		}
		return err
	}

	actionDone(c, fmt.Sprintf("PUT in chunks %q => %s\n", filePath, bck.Cname(objName)))

	return nil
}

// promptForChunking asks user whether to chunk a large file and returns chunk size.
// Returns: (chunkSize int64, shouldChunk bool, error)
// - If shouldChunk=false, use regular single upload
// - If shouldChunk=true, use chunkSize for splitting the file
func promptForChunking(c *cli.Context, fileSize int64, fileName string) (int64, bool, error) {
	// Show file size and default chunk size in human readable format
	fileSizeStr := cos.ToSizeIEC(fileSize, 2)
	defaultChunkStr := cos.ToSizeIEC(dfltChunkSize, 0)

	fmt.Fprintf(c.App.Writer, "\nLarge file detected: %s (%s)\n", fileName, fileSizeStr)
	fmt.Fprintln(c.App.Writer, "This file will be split into chunks for concurrent upload.")
	fmt.Fprintf(c.App.Writer, "Default chunk size: %s\n", defaultChunkStr)
	fmt.Fprintln(c.App.Writer, "\nOptions:")
	fmt.Fprintf(c.App.Writer, "  - Press ENTER or type 'yes' to use default chunk size (%s)\n", defaultChunkStr)
	fmt.Fprintln(c.App.Writer, "  - Type 'no' to upload without chunking")
	fmt.Fprintln(c.App.Writer, "  - Type a custom chunk size (e.g., '32MB', '64MiB', '1GB')")

	input := strings.TrimSpace(readValue(c, "Choice"))
	if input == "" {
		return dfltChunkSize, true, nil
	}

	// Check for no-chunking responses using cos.ParseBool
	if shouldChunk, err := cos.ParseBool(strings.ToLower(input)); err == nil {
		if !shouldChunk {
			fmt.Println("Proceeding with single upload (no chunking).")
			return 0, false, nil
		}
		// If user said "yes" but no size, use default chunk size
		return dfltChunkSize, true, nil
	}

	// Try to parse as custom chunk size
	if chunkSize, err := cos.ParseSize(input, cos.UnitsIEC); err == nil {
		if chunkSize > fileSize {
			return 0, false, fmt.Errorf("chunk size (%s) larger than file size (%s)", cos.ToSizeIEC(chunkSize, 0), fileSizeStr)
		}
		if chunkSize < chunkSizeMin {
			return 0, false, fmt.Errorf("chunk size (%s) smaller than minimum (%s)", cos.ToSizeIEC(chunkSize, 0), cos.ToSizeIEC(chunkSizeMin, 0))
		}
		if chunkSize > chunkSizeMax {
			return 0, false, fmt.Errorf("chunk size (%s) larger than maximum (%s)", cos.ToSizeIEC(chunkSize, 0), cos.ToSizeIEC(chunkSizeMax, 0))
		}
		fmt.Fprintf(c.App.Writer, "Using custom chunk size: %s\n", cos.ToSizeIEC(chunkSize, 0))
		return chunkSize, true, nil
	}

	return 0, false, errors.New("invalid input. Please enter ENTER (default), 'n/no' (no chunking), or a valid size (e.g., '32MB')")
}
