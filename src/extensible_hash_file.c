#include "../include/extensible_hash_file.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define HEF_MAX_GLOBAL_DEPTH 20u
#define HEF_MAX_DIRECTORY_ENTRIES (1u << HEF_MAX_GLOBAL_DEPTH)
#define HEF_DIRECTORY_RESERVED_BYTES ((uint64_t)HEF_MAX_DIRECTORY_ENTRIES * sizeof(uint64_t))
#define HEF_MAGIC 0x31464548u
#define HEF_VERSION 1u
#define HEF_MAX_KEY_LEN 63u
#define HEF_INVALID_OFFSET 0ULL

typedef struct
{
    uint32_t magic;
    uint32_t version;
    uint32_t bucket_capacity;
    uint32_t value_size;
    uint32_t global_depth;
    uint32_t directory_entry_count;
    uint32_t bucket_count;
    uint32_t size;
    uint64_t directory_offset;
    uint64_t next_bucket_offset;
} HefFileHeader;

typedef struct
{
    uint32_t local_depth;
    uint32_t capacity;
    uint32_t count;
    uint32_t value_size;
    uint32_t record_stride;
} HefBucketHeader;

typedef struct
{
    uint8_t in_use;
    uint8_t deleted;
    uint16_t reserved;
    uint32_t hash;
    char key[HEF_MAX_KEY_LEN + 1u];
} HefRecordHeader;

typedef struct
{
    FILE *fp;
    char *path;
    HefFileHeader header;
    uint64_t *directory;
    size_t record_size;
} HefHandle;

static HashExtStatus hef_write_header(HefHandle *hf);
static HashExtStatus hef_load_directory(HefHandle *hf);
static HashExtStatus hef_write_directory(HefHandle *hf);
static HashExtStatus hef_bucket_read_header(HefHandle *hf, uint64_t offset, HefBucketHeader *out);
static HashExtStatus hef_bucket_write_header(HefHandle *hf, uint64_t offset, const HefBucketHeader *bucket);
static HashExtStatus hef_bucket_read_record(HefHandle *hf, uint64_t bucket_offset, uint32_t index, void *buffer);
static HashExtStatus hef_bucket_write_record(HefHandle *hf, uint64_t bucket_offset, uint32_t index, const void *buffer);
static uint32_t hef_hash_key(const char *key);
static uint32_t hef_directory_index(const HefHandle *hf, uint32_t hash);
static HashExtStatus hef_find_record(HashExtFile hf,
                                     const char *key,
                                     uint64_t *out_bucket_offset,
                                     uint32_t *out_slot,
                                     void *record_buffer,
                                     HefBucketHeader *bucket_header);
static HashExtStatus hef_allocate_bucket(HefHandle *hf, uint32_t local_depth, uint64_t *out_offset);
static HashExtStatus hef_double_directory(HefHandle *hf);
static HashExtStatus hef_split_bucket(HefHandle *hf, uint32_t directory_index);
static HashExtStatus hef_insert_internal(HefHandle *hf, const char *key, const void *value, bool allow_update);
static HashExtStatus hef_bucket_compact_remove(HefHandle *hf,
                                               uint64_t bucket_offset,
                                               uint32_t slot,
                                               void *out_removed_value);
static char *hef_strdup_local(const char *src);

const char *hef_status_str(HashExtStatus status)
{
    switch (status)
    {
    case HEF_OK:
        return "HEF_OK";
    case HEF_ERR_INVALID_ARG:
        return "HEF_ERR_INVALID_ARG";
    case HEF_ERR_IO:
        return "HEF_ERR_IO";
    case HEF_ERR_NOT_FOUND:
        return "HEF_ERR_NOT_FOUND";
    case HEF_ERR_DUPLICATE_KEY:
        return "HEF_ERR_DUPLICATE_KEY";
    case HEF_ERR_CORRUPTED:
        return "HEF_ERR_CORRUPTED";
    case HEF_ERR_NO_MEMORY:
        return "HEF_ERR_NO_MEMORY";
    default:
        return "HEF_UNKNOWN_STATUS";
    }
}

HashExtStatus hef_create(const char *path,
                         uint32_t bucket_capacity,
                         uint32_t value_size,
                         uint32_t initial_global_depth,
                         HashExtFile *out_hf)
{
    HefHandle *hf = NULL;
    FILE *fp = NULL;
    uint32_t directory_entry_count;
    uint32_t i;

    if (path == NULL || out_hf == NULL || bucket_capacity == 0u || value_size == 0u || initial_global_depth > 20u)
    {
        return HEF_ERR_INVALID_ARG;
    }

    *out_hf = NULL;
    directory_entry_count = 1u << initial_global_depth;

    hf = (HefHandle *)calloc(1u, sizeof(*hf));
    if (hf == NULL)
    {
        return HEF_ERR_NO_MEMORY;
    }

    hf->path = hef_strdup_local(path);
    if (hf->path == NULL)
    {
        free(hf);
        return HEF_ERR_NO_MEMORY;
    }

    hf->record_size = sizeof(HefRecordHeader) + value_size;
    hf->directory = (uint64_t *)calloc(directory_entry_count, sizeof(uint64_t));
    if (hf->directory == NULL)
    {
        free(hf->path);
        free(hf);
        return HEF_ERR_NO_MEMORY;
    }

    fp = fopen(path, "w+b");
    if (fp == NULL)
    {
        free(hf->directory);
        free(hf->path);
        free(hf);
        return HEF_ERR_IO;
    }

    hf->fp = fp;
    hf->header.magic = HEF_MAGIC;
    hf->header.version = HEF_VERSION;
    hf->header.bucket_capacity = bucket_capacity;
    hf->header.value_size = value_size;
    hf->header.global_depth = initial_global_depth;
    hf->header.directory_entry_count = directory_entry_count;
    hf->header.bucket_count = directory_entry_count;
    hf->header.size = 0u;
    hf->header.directory_offset = sizeof(HefFileHeader);
    hf->header.next_bucket_offset = hf->header.directory_offset + HEF_DIRECTORY_RESERVED_BYTES;

    if (hef_write_header(hf) != HEF_OK || hef_write_directory(hf) != HEF_OK)
    {
        fclose(fp);
        free(hf->directory);
        free(hf->path);
        free(hf);
        return HEF_ERR_IO;
    }

    for (i = 0u; i < directory_entry_count; ++i)
    {
        uint64_t bucket_offset;
        HashExtStatus st = hef_allocate_bucket(hf, initial_global_depth, &bucket_offset);
        if (st != HEF_OK)
        {
            fclose(fp);
            free(hf->directory);
            free(hf->path);
            free(hf);
            return st;
        }
        hf->directory[i] = bucket_offset;
    }

    if (hef_write_directory(hf) != HEF_OK || hef_write_header(hf) != HEF_OK || fflush(fp) != 0)
    {
        fclose(fp);
        free(hf->directory);
        free(hf->path);
        free(hf);
        return HEF_ERR_IO;
    }

    *out_hf = hf;
    return HEF_OK;
}

HashExtStatus hef_open(const char *path, HashExtFile *out_hf)
{
    HefHandle *hf;
    FILE *fp;
    HashExtStatus st;

    if (path == NULL || out_hf == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    *out_hf = NULL;
    fp = fopen(path, "r+b");
    if (fp == NULL)
    {
        return HEF_ERR_IO;
    }

    hf = (HefHandle *)calloc(1u, sizeof(*hf));
    if (hf == NULL)
    {
        fclose(fp);
        return HEF_ERR_NO_MEMORY;
    }

    hf->path = hef_strdup_local(path);
    if (hf->path == NULL)
    {
        fclose(fp);
        free(hf);
        return HEF_ERR_NO_MEMORY;
    }

    hf->fp = fp;
    if (fseek(fp, 0L, SEEK_SET) != 0 || fread(&hf->header, sizeof(HefFileHeader), 1u, fp) != 1u)
    {
        fclose(fp);
        free(hf->path);
        free(hf);
        return HEF_ERR_IO;
    }

    if (hf->header.magic != HEF_MAGIC || hf->header.version != HEF_VERSION || hf->header.bucket_capacity == 0u || hf->header.value_size == 0u)
    {
        fclose(fp);
        free(hf->path);
        free(hf);
        return HEF_ERR_CORRUPTED;
    }

    hf->record_size = sizeof(HefRecordHeader) + hf->header.value_size;
    st = hef_load_directory(hf);
    if (st != HEF_OK)
    {
        fclose(fp);
        free(hf->path);
        free(hf);
        return st;
    }

    *out_hf = hf;
    return HEF_OK;
}

HashExtStatus hef_close(HashExtFile *io_hf)
{
    HefHandle *hf;

    if (io_hf == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    hf = (HefHandle *)(*io_hf);
    if (hf == NULL)
    {
        return HEF_OK;
    }

    if (hef_flush(hf) != HEF_OK)
    {
        return HEF_ERR_IO;
    }

    fclose(hf->fp);
    free(hf->directory);
    free(hf->path);
    free(hf);
    *io_hf = NULL;
    return HEF_OK;
}

HashExtStatus hef_flush(HashExtFile hf_ptr)
{
    HefHandle *hf = (HefHandle *)hf_ptr;

    if (hf == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    if (hef_write_header(hf) != HEF_OK || hef_write_directory(hf) != HEF_OK || fflush(hf->fp) != 0)
    {
        return HEF_ERR_IO;
    }

    return HEF_OK;
}

HashExtStatus hef_insert(HashExtFile hf_ptr, const char *key, const void *value)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    if (hf == NULL || key == NULL || value == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }
    return hef_insert_internal(hf, key, value, false);
}

HashExtStatus hef_get(HashExtFile hf_ptr, const char *key, void *out_value)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    uint8_t *record_buffer;
    HashExtStatus st;

    if (hf == NULL || key == NULL || out_value == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    record_buffer = (uint8_t *)malloc(hf->record_size);
    if (record_buffer == NULL)
    {
        return HEF_ERR_NO_MEMORY;
    }

    st = hef_find_record(hf, key, NULL, NULL, record_buffer, NULL);
    if (st == HEF_OK)
    {
        memcpy(out_value, record_buffer + sizeof(HefRecordHeader), hf->header.value_size);
    }

    free(record_buffer);
    return st;
}

HashExtStatus hef_update(HashExtFile hf_ptr, const char *key, const void *value)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    uint64_t bucket_offset;
    uint32_t slot;
    uint8_t *record_buffer;
    HefRecordHeader *record_header;
    HashExtStatus st;

    if (hf == NULL || key == NULL || value == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    record_buffer = (uint8_t *)malloc(hf->record_size);
    if (record_buffer == NULL)
    {
        return HEF_ERR_NO_MEMORY;
    }

    st = hef_find_record(hf, key, &bucket_offset, &slot, record_buffer, NULL);
    if (st != HEF_OK)
    {
        free(record_buffer);
        return st;
    }

    record_header = (HefRecordHeader *)record_buffer;
    record_header->deleted = 0u;
    memcpy(record_buffer + sizeof(HefRecordHeader), value, hf->header.value_size);
    st = hef_bucket_write_record(hf, bucket_offset, slot, record_buffer);
    free(record_buffer);
    if (st != HEF_OK)
    {
        return st;
    }

    return hef_flush(hf);
}

HashExtStatus hef_remove(HashExtFile hf_ptr, const char *key, void *out_removed_value)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    uint64_t bucket_offset;
    uint32_t slot;
    HashExtStatus st;

    if (hf == NULL || key == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    st = hef_find_record(hf, key, &bucket_offset, &slot, NULL, NULL);
    if (st != HEF_OK)
    {
        return st;
    }

    st = hef_bucket_compact_remove(hf, bucket_offset, slot, out_removed_value);
    if (st != HEF_OK)
    {
        return st;
    }

    if (hf->header.size > 0u)
    {
        hf->header.size--;
    }

    return hef_flush(hf);
}

HashExtStatus hef_contains(HashExtFile hf_ptr, const char *key, bool *out_contains)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    HashExtStatus st;

    if (hf == NULL || key == NULL || out_contains == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    st = hef_find_record(hf, key, NULL, NULL, NULL, NULL);
    if (st == HEF_OK)
    {
        *out_contains = true;
        return HEF_OK;
    }
    if (st == HEF_ERR_NOT_FOUND)
    {
        *out_contains = false;
        return HEF_OK;
    }
    return st;
}

HashExtStatus hef_size(HashExtFile hf_ptr, uint32_t *out_size)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    if (hf == NULL || out_size == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }
    *out_size = hf->header.size;
    return HEF_OK;
}

HashExtStatus hef_value_size(HashExtFile hf_ptr, uint32_t *out_value_size)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    if (hf == NULL || out_value_size == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }
    *out_value_size = hf->header.value_size;
    return HEF_OK;
}

HashExtStatus hef_global_depth(HashExtFile hf_ptr, uint32_t *out_depth)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    if (hf == NULL || out_depth == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }
    *out_depth = hf->header.global_depth;
    return HEF_OK;
}

HashExtStatus hef_bucket_count(HashExtFile hf_ptr, uint32_t *out_count)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    if (hf == NULL || out_count == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }
    *out_count = hf->header.bucket_count;
    return HEF_OK;
}

HashExtStatus hef_directory_entry_count(HashExtFile hf_ptr, uint32_t *out_count)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    if (hf == NULL || out_count == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }
    *out_count = hf->header.directory_entry_count;
    return HEF_OK;
}

static HashExtStatus hef_write_header(HefHandle *hf)
{
    if (fseek(hf->fp, 0L, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fwrite(&hf->header, sizeof(HefFileHeader), 1u, hf->fp) != 1u)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static HashExtStatus hef_load_directory(HefHandle *hf)
{
    size_t bytes = (size_t)hf->header.directory_entry_count * sizeof(uint64_t);
    hf->directory = (uint64_t *)malloc(bytes);
    if (hf->directory == NULL)
    {
        return HEF_ERR_NO_MEMORY;
    }
    if (fseek(hf->fp, (long)hf->header.directory_offset, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fread(hf->directory, sizeof(uint64_t), hf->header.directory_entry_count, hf->fp) != hf->header.directory_entry_count)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static HashExtStatus hef_write_directory(HefHandle *hf)
{
    if (fseek(hf->fp, (long)hf->header.directory_offset, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fwrite(hf->directory, sizeof(uint64_t), hf->header.directory_entry_count, hf->fp) != hf->header.directory_entry_count)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static HashExtStatus hef_bucket_read_header(HefHandle *hf, uint64_t offset, HefBucketHeader *out)
{
    if (fseek(hf->fp, (long)offset, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fread(out, sizeof(HefBucketHeader), 1u, hf->fp) != 1u)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static HashExtStatus hef_bucket_write_header(HefHandle *hf, uint64_t offset, const HefBucketHeader *bucket)
{
    if (fseek(hf->fp, (long)offset, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fwrite(bucket, sizeof(HefBucketHeader), 1u, hf->fp) != 1u)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static HashExtStatus hef_bucket_read_record(HefHandle *hf, uint64_t bucket_offset, uint32_t index, void *buffer)
{
    uint64_t record_offset = bucket_offset + sizeof(HefBucketHeader) + ((uint64_t)index * hf->record_size);
    if (fseek(hf->fp, (long)record_offset, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fread(buffer, hf->record_size, 1u, hf->fp) != 1u)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static HashExtStatus hef_bucket_write_record(HefHandle *hf, uint64_t bucket_offset, uint32_t index, const void *buffer)
{
    uint64_t record_offset = bucket_offset + sizeof(HefBucketHeader) + ((uint64_t)index * hf->record_size);
    if (fseek(hf->fp, (long)record_offset, SEEK_SET) != 0)
    {
        return HEF_ERR_IO;
    }
    if (fwrite(buffer, hf->record_size, 1u, hf->fp) != 1u)
    {
        return HEF_ERR_IO;
    }
    return HEF_OK;
}

static uint32_t hef_hash_key(const char *key)
{
    uint32_t hash = 2166136261u;
    while (*key != '\0')
    {
        hash ^= (unsigned char)(*key);
        hash *= 16777619u;
        ++key;
    }
    return hash;
}

static uint32_t hef_directory_index(const HefHandle *hf, uint32_t hash)
{
    uint32_t mask = (1u << hf->header.global_depth) - 1u;
    return hash & mask;
}

static HashExtStatus hef_find_record(HashExtFile hf_ptr,
                                     const char *key,
                                     uint64_t *out_bucket_offset,
                                     uint32_t *out_slot,
                                     void *record_buffer,
                                     HefBucketHeader *bucket_header)
{
    HefHandle *hf = (HefHandle *)hf_ptr;
    uint32_t index;
    uint64_t bucket_offset;
    HefBucketHeader bucket;
    uint8_t *temp = NULL;
    uint32_t i;

    if (hf == NULL || key == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    index = hef_directory_index(hf, hef_hash_key(key));
    bucket_offset = hf->directory[index];

    if (hef_bucket_read_header(hf, bucket_offset, &bucket) != HEF_OK)
    {
        return HEF_ERR_IO;
    }

    if (bucket.count > bucket.capacity || bucket.value_size != hf->header.value_size)
    {
        return HEF_ERR_CORRUPTED;
    }

    if (record_buffer == NULL)
    {
        temp = (uint8_t *)malloc(hf->record_size);
        if (temp == NULL)
        {
            return HEF_ERR_NO_MEMORY;
        }
        record_buffer = temp;
    }

    for (i = 0u; i < bucket.count; ++i)
    {
        HefRecordHeader *rec_header;
        if (hef_bucket_read_record(hf, bucket_offset, i, record_buffer) != HEF_OK)
        {
            free(temp);
            return HEF_ERR_IO;
        }
        rec_header = (HefRecordHeader *)record_buffer;
        if (rec_header->in_use == 1u && rec_header->deleted == 0u && strncmp(rec_header->key, key, HEF_MAX_KEY_LEN) == 0)
        {
            if (out_bucket_offset != NULL)
            {
                *out_bucket_offset = bucket_offset;
            }
            if (out_slot != NULL)
            {
                *out_slot = i;
            }
            if (bucket_header != NULL)
            {
                *bucket_header = bucket;
            }
            free(temp);
            return HEF_OK;
        }
    }

    if (bucket_header != NULL)
    {
        *bucket_header = bucket;
    }
    if (out_bucket_offset != NULL)
    {
        *out_bucket_offset = bucket_offset;
    }
    free(temp);
    return HEF_ERR_NOT_FOUND;
}

static HashExtStatus hef_allocate_bucket(HefHandle *hf, uint32_t local_depth, uint64_t *out_offset)
{
    HefBucketHeader bucket;
    uint8_t *zero_record;
    uint64_t offset;
    uint32_t i;

    if (out_offset == NULL)
    {
        return HEF_ERR_INVALID_ARG;
    }

    offset = hf->header.next_bucket_offset;
    bucket.local_depth = local_depth;
    bucket.capacity = hf->header.bucket_capacity;
    bucket.count = 0u;
    bucket.value_size = hf->header.value_size;
    bucket.record_stride = (uint32_t)hf->record_size;

    if (hef_bucket_write_header(hf, offset, &bucket) != HEF_OK)
    {
        return HEF_ERR_IO;
    }

    zero_record = (uint8_t *)calloc(1u, hf->record_size);
    if (zero_record == NULL)
    {
        return HEF_ERR_NO_MEMORY;
    }

    for (i = 0u; i < bucket.capacity; ++i)
    {
        if (hef_bucket_write_record(hf, offset, i, zero_record) != HEF_OK)
        {
            free(zero_record);
            return HEF_ERR_IO;
        }
    }

    free(zero_record);
    hf->header.next_bucket_offset += sizeof(HefBucketHeader) + ((uint64_t)bucket.capacity * hf->record_size);
    *out_offset = offset;
    return HEF_OK;
}

static HashExtStatus hef_double_directory(HefHandle *hf)
{
    uint32_t old_count = hf->header.directory_entry_count;
    uint32_t new_count = old_count * 2u;
    uint64_t *new_directory;
    uint32_t i;

    if (new_count < old_count)
    {
        return HEF_ERR_CORRUPTED;
    }

    new_directory = (uint64_t *)realloc(hf->directory, (size_t)new_count * sizeof(uint64_t));
    if (new_directory == NULL)
    {
        return HEF_ERR_NO_MEMORY;
    }
    hf->directory = new_directory;

    for (i = 0u; i < old_count; ++i)
    {
        hf->directory[i + old_count] = hf->directory[i];
    }

    hf->header.global_depth++;
    hf->header.directory_entry_count = new_count;
    return HEF_OK;
}

static HashExtStatus hef_split_bucket(HefHandle *hf, uint32_t directory_index)
{
    uint64_t old_bucket_offset = hf->directory[directory_index];
    HefBucketHeader old_bucket;
    HefBucketHeader new_bucket_header;
    uint64_t new_bucket_offset;
    uint8_t *record_buffer;
    uint8_t *values_blob;
    char *keys_blob;
    uint32_t record_count;
    uint32_t i;
    HashExtStatus st;

    st = hef_bucket_read_header(hf, old_bucket_offset, &old_bucket);
    if (st != HEF_OK)
    {
        return st;
    }

    if (old_bucket.local_depth == hf->header.global_depth)
    {
        st = hef_double_directory(hf);
        if (st != HEF_OK)
        {
            return st;
        }
    }

    st = hef_allocate_bucket(hf, old_bucket.local_depth + 1u, &new_bucket_offset);
    if (st != HEF_OK)
    {
        return st;
    }

    record_buffer = (uint8_t *)malloc(hf->record_size);
    keys_blob = (char *)calloc(old_bucket.count, HEF_MAX_KEY_LEN + 1u);
    values_blob = (uint8_t *)calloc(old_bucket.count, hf->header.value_size);
    if (record_buffer == NULL || keys_blob == NULL || values_blob == NULL)
    {
        free(record_buffer);
        free(keys_blob);
        free(values_blob);
        return HEF_ERR_NO_MEMORY;
    }

    record_count = old_bucket.count;
    for (i = 0u; i < record_count; ++i)
    {
        HefRecordHeader *rh;
        st = hef_bucket_read_record(hf, old_bucket_offset, i, record_buffer);
        if (st != HEF_OK)
        {
            free(record_buffer);
            free(keys_blob);
            free(values_blob);
            return st;
        }
        rh = (HefRecordHeader *)record_buffer;
        strncpy(keys_blob + ((size_t)i * (HEF_MAX_KEY_LEN + 1u)), rh->key, HEF_MAX_KEY_LEN);
        memcpy(values_blob + ((size_t)i * hf->header.value_size), record_buffer + sizeof(HefRecordHeader), hf->header.value_size);
    }

    old_bucket.local_depth++;
    old_bucket.count = 0u;
    st = hef_bucket_write_header(hf, old_bucket_offset, &old_bucket);
    if (st != HEF_OK)
    {
        free(record_buffer);
        free(keys_blob);
        free(values_blob);
        return st;
    }

    memset(record_buffer, 0, hf->record_size);
    for (i = 0u; i < old_bucket.capacity; ++i)
    {
        st = hef_bucket_write_record(hf, old_bucket_offset, i, record_buffer);
        if (st != HEF_OK)
        {
            free(record_buffer);
            free(keys_blob);
            free(values_blob);
            return st;
        }
    }

    new_bucket_header.local_depth = old_bucket.local_depth;
    new_bucket_header.capacity = hf->header.bucket_capacity;
    new_bucket_header.count = 0u;
    new_bucket_header.value_size = hf->header.value_size;
    new_bucket_header.record_stride = (uint32_t)hf->record_size;
    st = hef_bucket_write_header(hf, new_bucket_offset, &new_bucket_header);
    if (st != HEF_OK)
    {
        free(record_buffer);
        free(keys_blob);
        free(values_blob);
        return st;
    }

    for (i = 0u; i < hf->header.directory_entry_count; ++i)
    {
        if (hf->directory[i] == old_bucket_offset)
        {
            if (((i >> (old_bucket.local_depth - 1u)) & 1u) != 0u)
            {
                hf->directory[i] = new_bucket_offset;
            }
        }
    }

    hf->header.bucket_count++;

    for (i = 0u; i < record_count; ++i)
    {
        st = hef_insert_internal(hf,
                                 keys_blob + ((size_t)i * (HEF_MAX_KEY_LEN + 1u)),
                                 values_blob + ((size_t)i * hf->header.value_size),
                                 false);
        if (st != HEF_OK)
        {
            free(record_buffer);
            free(keys_blob);
            free(values_blob);
            return st;
        }
        if (hf->header.size > 0u)
        {
            hf->header.size--;
        }
    }

    free(record_buffer);
    free(keys_blob);
    free(values_blob);
    return HEF_OK;
}

static HashExtStatus hef_insert_internal(HefHandle *hf, const char *key, const void *value, bool allow_update)
{
    uint32_t hash;
    uint32_t directory_index;
    uint64_t bucket_offset;
    HefBucketHeader bucket;
    uint8_t *record_buffer;
    HefRecordHeader *record_header;
    HashExtStatus st;

    if (strlen(key) > HEF_MAX_KEY_LEN)
    {
        return HEF_ERR_INVALID_ARG;
    }

    if (hef_find_record(hf, key, &bucket_offset, NULL, NULL, &bucket) == HEF_OK)
    {
        if (!allow_update)
        {
            return HEF_ERR_DUPLICATE_KEY;
        }
        return hef_update(hf, key, value);
    }

    while (1)
    {
        hash = hef_hash_key(key);
        directory_index = hef_directory_index(hf, hash);
        bucket_offset = hf->directory[directory_index];

        st = hef_bucket_read_header(hf, bucket_offset, &bucket);
        if (st != HEF_OK)
        {
            return st;
        }

        if (bucket.count < bucket.capacity)
        {
            record_buffer = (uint8_t *)calloc(1u, hf->record_size);
            if (record_buffer == NULL)
            {
                return HEF_ERR_NO_MEMORY;
            }
            record_header = (HefRecordHeader *)record_buffer;
            record_header->in_use = 1u;
            record_header->deleted = 0u;
            record_header->hash = hash;
            strncpy(record_header->key, key, HEF_MAX_KEY_LEN);
            memcpy(record_buffer + sizeof(HefRecordHeader), value, hf->header.value_size);

            st = hef_bucket_write_record(hf, bucket_offset, bucket.count, record_buffer);
            free(record_buffer);
            if (st != HEF_OK)
            {
                return st;
            }

            bucket.count++;
            st = hef_bucket_write_header(hf, bucket_offset, &bucket);
            if (st != HEF_OK)
            {
                return st;
            }
            hf->header.size++;
            return hef_flush(hf);
        }

        st = hef_split_bucket(hf, directory_index);
        if (st != HEF_OK)
        {
            return st;
        }
    }
}

static HashExtStatus hef_bucket_compact_remove(HefHandle *hf,
                                               uint64_t bucket_offset,
                                               uint32_t slot,
                                               void *out_removed_value)
{
    HefBucketHeader bucket;
    uint8_t *target_record;
    uint8_t *last_record;
    HashExtStatus st;

    st = hef_bucket_read_header(hf, bucket_offset, &bucket);
    if (st != HEF_OK)
    {
        return st;
    }
    if (bucket.count == 0u || slot >= bucket.count)
    {
        return HEF_ERR_NOT_FOUND;
    }

    target_record = (uint8_t *)malloc(hf->record_size);
    last_record = (uint8_t *)malloc(hf->record_size);
    if (target_record == NULL || last_record == NULL)
    {
        free(target_record);
        free(last_record);
        return HEF_ERR_NO_MEMORY;
    }

    st = hef_bucket_read_record(hf, bucket_offset, slot, target_record);
    if (st != HEF_OK)
    {
        free(target_record);
        free(last_record);
        return st;
    }

    if (out_removed_value != NULL)
    {
        memcpy(out_removed_value, target_record + sizeof(HefRecordHeader), hf->header.value_size);
    }

    if (slot != bucket.count - 1u)
    {
        st = hef_bucket_read_record(hf, bucket_offset, bucket.count - 1u, last_record);
        if (st != HEF_OK)
        {
            free(target_record);
            free(last_record);
            return st;
        }
        st = hef_bucket_write_record(hf, bucket_offset, slot, last_record);
        if (st != HEF_OK)
        {
            free(target_record);
            free(last_record);
            return st;
        }
    }

    memset(last_record, 0, hf->record_size);
    st = hef_bucket_write_record(hf, bucket_offset, bucket.count - 1u, last_record);
    free(target_record);
    free(last_record);
    if (st != HEF_OK)
    {
        return st;
    }

    bucket.count--;
    return hef_bucket_write_header(hf, bucket_offset, &bucket);
}

static char *hef_strdup_local(const char *src)
{
    size_t len;
    char *copy;
    if (src == NULL)
    {
        return NULL;
    }
    len = strlen(src);
    copy = (char *)malloc(len + 1u);
    if (copy == NULL)
    {
        return NULL;
    }
    memcpy(copy, src, len + 1u);
    return copy;
}
