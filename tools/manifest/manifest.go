package main

import (
    "fmt"
    "io"
    "os"
    "sort"
    "strings"
    "errors"
    "time"
    "io/fs"
    "bytes"
)

func main() {
    // detect flags
    var zero bool
    var nomoreflags bool
    var args []string
    for _, arg := range os.Args[1:] {
        if nomoreflags {
            args = append(args, arg)
            continue
        }
        switch arg {
        case "-0":
            zero = true
        case "--":
            nomoreflags = true
        default:
            args = append(args, arg)
        }
    }

    if len(args) < 1 {
        fmt.Fprintf(os.Stderr, "usage: manifest OUTPUT [-0] < filelist\n")
        os.Exit(1)
    }

    output := args[0]

    byts, err := io.ReadAll(os.Stdin)
    if err != nil {
        fmt.Fprintf(os.Stderr, "error reading from stdin: %v\n", err)
        os.Exit(1)
    }

    var sep string
    if zero {
        sep = "\000"
    } else {
        sep = "\n"
    }

    files := strings.Split(strings.Trim(string(byts), sep), sep)

    // sort the input files
    sort.Strings(files)

    joined := []byte(strings.Join(files, sep))
    joined = append(joined, sep...)

    // load the output file
    byts, err = os.ReadFile(output)
    if err != nil {
        if !errors.Is(err, fs.ErrNotExist) {
            fmt.Fprintf(os.Stderr, "error reading from %v: %v\n", output, err)
            os.Exit(1)
        }
        // output doesn't exist yet, just write it and quit
        err = os.WriteFile(output, joined, 0666)
        if err != nil {
            fmt.Fprintf(os.Stderr, "error writing to %v: %v\n", output, err)
            os.Exit(1)
        }
        return
    }

    if !bytes.Equal(byts, joined) {
        // output has out-of-date contents, overwrite and quit
        err = os.WriteFile(output, joined, 0666)
        if err != nil {
            fmt.Fprintf(os.Stderr, "error writing to %v: %v\n", output, err)
            os.Exit(1)
        }
        return
    }

    // make sure that the output file is newer than any file in the list

    outInfo, err := os.Stat(output)
    if err != nil {
        fmt.Fprintf(os.Stderr, "stat(%v): %v\n", output, err)
        os.Exit(1)
    }

    for _, file := range files {
        info, err := os.Stat(file)
        if err != nil {
            fmt.Fprintf(os.Stderr, "stat(%v): %v\n", file, err)
            os.Exit(1)
        }

        if info.ModTime().After(outInfo.ModTime()) {
            // update the output file modified-time and exit
            now := time.Now()
            err = os.Chtimes(output, now, now)
            if err != nil {
                fmt.Fprintf(os.Stderr, "os.Chtimes(%v): %v\n", output, err)
                os.Exit(1)
            }
        }
    }
}
