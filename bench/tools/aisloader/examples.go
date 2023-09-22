// Package aisloader
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

// Examples:
// 1. Destroy existing ais bucket:
//    a) aisloader -bucket=ais://abc -duration 0s -totalputsize=0 -cleanup=true
//    Delete all objects in a given AWS-based bucket:
//    b) aisloader -bucket=aws://nvais -cleanup=true -duration 0s -totalputsize=0
// 2. Timed (for 1h) 100% GET from a Cloud bucket (no cleanup):
//    aisloader -bucket=aws://mybucket -duration 1h -numworkers=30 -pctput=0 -cleanup=false
// 3. Time-based PUT into ais bucket, random objects names:
//    aisloader -bucket=abc -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais -cleanup=true
// 4. Mixed 30% PUT and 70% GET to/from a Cloud bucket. PUT will generate random object names and is limited by 10GB total size:
//    aisloader -bucket=gs://nvgs -duration 0s -numworkers=3 -minsize=1MB -maxsize=1MB -pctput=30 -totalputsize=10G -cleanup=false
// 5. PUT 2000 objects with names matching hex({0..2000}{loaderid}) template:
//    aisloader -bucket=ais://abc -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -cleanup=false
// ====================
// 6. Use random object names and loaderID for reporting stats
//    aisloader -loaderid=10
// 7. PUT objects with names based on loaderID and total number of loaders; names: hex({0..}{loaderid})
//    aisloader -loaderid=10 -loadernum=20
// 8. PUT objects with names passed on hash of loaderID of length -loaderidhashlen; names: hex({0..}{hash(loaderid)})
//    aisloader -loaderid=loaderstring -loaderidhashlen=8
// 9. Does nothing but prints loaderID:
//    aisloader -getloaderid (0x0)
//    aisloader -loaderid=10 -getloaderid (0xa)
//    aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid (0xdb)
// ====================
// 10. Timed 100% GET from s3 bucket directly (NOTE: aistore is not being used here):
//    aisloader -bucket=s3://xyz -numworkers=8 -pctput=0 -duration=10m -s3endpoint=https://s3.amazonaws.com
// 11. PUT approx. 8000 files into s3 bucket directly, skip printing usage and defaults
//     (NOTE: aistore is not being used):
// aisloader -bucket=s3://xyz -minsize=16B -maxsize=16B -numworkers=8 -pctput=100 -totalputsize=128k -s3endpoint=https://s3.amazonaws.com -quiet
// =====================
// 12. Generate a list of object names (once), and then run aisloader without executing list-objects
// ais ls ais://nnn --props name -H > /tmp/a.txt
// aisloader -bucket=ais://nnn -duration 1h -numworkers=30 -pctput=0 -filelist /tmp/a.txt -cleanup=false
//
