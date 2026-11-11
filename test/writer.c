// writer.c - deterministic checkpoint writer/verifier for bblite tests.
// usage: writer plain|fsync|stdio|rewrite|verify <path> <MB>
// content: byte at offset k == (k*31 + k/8128) & 0xff  (position-dependent)
#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CHUNK (1024 * 1024)

static unsigned char byte_at(long k) { return (unsigned char)((k * 31 + k / 8128) & 0xff); }

static void fill(unsigned char* buf, long off)
{
    for (int i = 0; i < CHUNK; i++)
        buf[i] = byte_at(off + i);
}

static int write_pass(const char* path, long total, int do_fsync)
{
    unsigned char* buf = malloc(CHUNK);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); return 1; }
    for (long off = 0; off < total; off += CHUNK) {
        fill(buf, off);
        long put = 0;
        while (put < CHUNK) {
            ssize_t w = write(fd, buf + put, CHUNK - put);
            if (w <= 0) { perror("write"); return 1; }
            put += w;
        }
    }
    free(buf);
    if (do_fsync && fsync(fd) != 0) { perror("fsync"); return 1; }
    if (close(fd) != 0) { perror("close"); return 1; }
    return 0;
}

int main(int argc, char** argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: %s plain|fsync|stdio|rewrite|verify <path> <MB>\n", argv[0]);
        return 2;
    }
    const char* mode = argv[1];
    const char* path = argv[2];
    const long total = atol(argv[3]) * 1024L * 1024L;

    if (!strcmp(mode, "plain") || !strcmp(mode, "fsync"))
        return write_pass(path, total, !strcmp(mode, "fsync"));

    if (!strcmp(mode, "rewrite")) {   // same slot, 3 versions, in one process
        for (int pass = 0; pass < 3; pass++)
            if (write_pass(path, total, 0))
                return 1;
        return 0;
    }

    unsigned char* buf = malloc(CHUNK);
    if (!strcmp(mode, "stdio")) {
        FILE* f = fopen(path, "w");
        if (!f) { perror("fopen"); return 1; }
        for (long off = 0; off < total; off += CHUNK) {
            fill(buf, off);
            if (fwrite(buf, 1, CHUNK, f) != CHUNK) { fprintf(stderr, "fwrite short\n"); return 1; }
        }
        if (fclose(f) != 0) { perror("fclose"); return 1; }
        return 0;
    }

    if (!strcmp(mode, "verify")) {
        int fd = open(path, O_RDONLY);
        if (fd < 0) { perror("open verify"); return 1; }
        long off = 0, got;
        while ((got = read(fd, buf, CHUNK)) > 0) {
            for (long i = 0; i < got; i++)
                if (buf[i] != byte_at(off + i)) {
                    fprintf(stderr, "VERIFY MISMATCH at byte %ld\n", off + i);
                    return 1;
                }
            off += got;
        }
        unsigned char t;
        long extra = 0;
        while (read(fd, &t, 1) == 1) extra++;
        if (off != total || extra != 0) {
            fprintf(stderr, "VERIFY wrong size: %ld bytes (+%ld trailing), want %ld\n",
                    off, extra, total);
            return 1;
        }
        printf("VERIFY OK\n");
        return 0;
    }
    return 2;
}
