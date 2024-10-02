---
layout: post
title:  "Go: append a file to a TAR archive"
date:   2021-08-10 14:46:10 +0200
author: Vladimir Markelov
categories: golang archive tar
---

## The problem

AIStore supports a whole gamut of "archival" operations that allow to read, write, and list archives such as .tar, .tgz, and .zip. When we started working on **appending** content to existing archives, we quickly discovered that, surprisingly, the corresponding open source appears to be missing. Standard Go packages - e.g., [archive/tar](https://pkg.go.dev/archive/tar) - fully support creating and reading archives but _not_ appending to an existing one...

Looking for a solution on the Internet did not help - snippets of an open code that we could find did not work or worked only under certain restricted conditions.

In this text, we show how to append a file to an existing TAR. GitHub references are included below.

## First attempts

The first idea was to open an archive for appending and write new data at the end.
It did not work: a new file was missing in the archive list and the appended file was inaccessible.
TAR specification states:

> A tar archive consists of a series of 512-byte records.
> Each file system object requires a header record which stores basic metadata (pathname, owner, permissions, etc.) and zero or more records containing any file data.
> The end of the archive is indicated by two records consisting entirely of zero bytes.

Every TAR archive ends with an end of archive marker (a trailer): 2 zero blocks at the end.
Any information written after the trailer is ignored.
It made clear that a header and data of a new file had to overwrite the trailing zero blocks.
As the trailer size was 2 records, it seemed sufficient to start writing the new data with 1 KiB offset from the end of the archive.
A solution found on the Internet [employed this idea](https://stackoverflow.com/questions/18323995/golang-append-file-to-an-existing-tar-archive):

```go
const recordSize = 512
var data []byte
f, err := os.OpenFile("test.tar", os.O_RDWR, os.ModePerm)
if err != nil {
    log.Fatalln(err)
}
if _, err = f.Seek(-2 * recordSize, io.SeekEnd); err != nil {
    log.Fatalln(err)
}
tw := tar.NewWriter(f)
hdr := &tar.Header{
    Name: "new_file",
    Size: int64(len(data)),
}
if err := tw.WriteHeader(hdr); err != nil {
    log.Fatalln(err)
}
if _, err := tw.Write(data); err != nil {
    log.Fatalln(err)
}
tw.Close()
f.Close()
```

But the story did not end here. It worked fine only with TAR's created with Go standard library.
When I tried to append a new file to an archive created with a system `tar` utility, it failed:
the appended file was missing again.

## The solution

Digging into the trouble, I discovered that the number of zero blocks in the archive trailer depended on TAR version and defaults.
Go package added only 1 KiB of zeros, but the archive created with system `tar` had more than 4 KiB zeroes at the end.
That was why the first way did not work with an arbitrary TAR archive.
TAR did not seem to store information about trailer anywhere, so I had to calculate the size of the trailer somehow.
My final solution was inefficient for archives with a lot of files, yet it was reliable and it worked with any TAR archive:

1. Open an archive.
2. Pass its file handle to a TAR reader.
3. Iterate through all files inside the archive until `io.EOF` is reached.
4. For each file the TAR reader reports the file size, and the file pointer returns the position from which the file starts.
5. When TAR reader returns `io.EOF`, the file pointer is already beyond the zero trailer. So we have to use numbers from the previous iteration to calculate the end of archive data.

A tricky thing that the next archive entry must be written from the position aligned to TAR record boundary - 512 bytes.
So the file size must be rounded up to the nearest multiple of TAR record size.

```go
const recordSize = 512
var data []byte
fh, err := os.OpenFile("test.tar", os.O_RDWR, os.ModePerm)
if err != nil {
    log.Fatalln(err)
}
var (
	lastPos, lastSize int64
	err error
)
twr := tar.NewReader(fh)
for {
	st, err := twr.Next()
	if err != nil {
		if err == io.EOF {
			break
		}
		log.Fatalln(err)
	}
	if lastPos, err = fh.Seek(0, io.SeekCurrent); err != nil {
		log.Fatalln(err)
	}
	lastSize = st.Size
}
// Round up the size of the last file to multiple of recordSize
paddedSize := ((lastSize - 1) / recordSize + 1) * recordSize
if _, err = fh.Seek(lastPos+paddedSize, io.SeekStart); err != nil {
	log.Fatalln(err)
}

tw := tar.NewWriter(f)
hdr := &tar.Header{
    Name: "new_file",
    Size: int64(len(data)),
}
if err = tw.WriteHeader(hdr); err != nil {
    log.Fatalln(err)
}
if _, err = tw.Write(data); err != nil {
    log.Fatalln(err)
}
tw.Close()
fh.Close()
```

## References

For the latest code, please see:

- The function `OpenTarForAppend` in ["cos" package](https://github.com/NVIDIA/aistore/blob/main/xact/xs/archive.go).
- Example of how to use `OpenTarForAppend` in the implementation of the function `appendToArch` in the [core package](https://github.com/NVIDIA/aistore/blob/main/ais/tgtobj.go).
